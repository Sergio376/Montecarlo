"""Nodo trabajador: toma lotes de la cola, simula en paralelo y guarda el resultado en Redis.

Tolerancia a fallos:
  - ack manual: si el worker muere antes de confirmar, RabbitMQ regresa el lote a la cola
  - el resultado de cada lote es determinista (semilla = job, lote, chunk) y se guarda
    con una clave fija, así que procesar un lote dos veces no cambia nada (idempotente)
  - reintentos con espera creciente y cola de mensajes muertos (mc_tasks.dead)
  - heartbeat en Redis cada 3 s con TTL de 10 s
"""
import json
import os
import random
import socket
import threading
import time
from multiprocessing import Pool

import numpy as np
import pika
import psutil
import redis
from prometheus_client import REGISTRY, Counter, Histogram, start_http_server
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_USER = os.getenv("RABBIT_USER", "mc")
RABBIT_PASS = os.getenv("RABBIT_PASS", "mcpass")
REDIS_HOST = os.getenv("REDIS_HOST", "redis-primary")
PROCS = int(os.getenv("WORKER_PROCESSES", "2"))
CHUNK = int(os.getenv("CHUNK_SIZE", "25000"))     # simulaciones por tarea del pool
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
FAIL_RATE = float(os.getenv("FAIL_RATE", "0"))    # para inyectar errores de prueba
QUEUE, DLQ = "mc_tasks", "mc_tasks.dead"
TTL = 3600
HB_EVERY, HB_TTL = 3, 10
TRADING_DAYS = 252
WORKER_ID = socket.gethostname()

r = redis.Redis(host=REDIS_HOST, port=6379)
pool = None

# ---------- Métricas ----------
BATCHES = Counter("mc_batches_total", "Lotes procesados", ["status"])
SIMS = Counter("mc_simulations_total", "Simulaciones completadas")
JOBS_DONE = Counter("mc_jobs_completed_total", "Solicitudes terminadas (las cuenta quien termina el último lote)")
BATCH_TIME = Histogram("mc_batch_duration_seconds", "Duración de un lote",
                       buckets=(.1, .25, .5, 1, 2, 5, 10, 30))
JOB_TIME = Histogram("mc_job_duration_seconds", "Tiempo de una solicitud: llegada -> último lote",
                     buckets=(.25, .5, 1, 2, 5, 10, 20, 30, 60, 120))


class ProcCollector:
    """CPU y memoria del worker incluyendo los procesos hijos del pool."""

    def __init__(self):
        self.parent = psutil.Process()

    def collect(self):
        cpu = rss = 0.0
        for p in [self.parent] + self.parent.children(recursive=True):
            try:
                t = p.cpu_times()
                cpu += t.user + t.system
                rss += p.memory_info().rss
            except psutil.Error:
                pass
        yield CounterMetricFamily("worker_cpu_seconds", "CPU acumulada del worker y su pool", value=cpu)
        yield GaugeMetricFamily("worker_memory_rss_bytes", "Memoria residente del worker y su pool", value=rss)


def simulate_chunk(args):
    """Simula `n` escenarios de movimiento browniano geométrico y regresa las pérdidas."""
    n, s0, mu, sigma, days, entropy = args
    rng = np.random.default_rng(np.random.SeedSequence(entropy))
    t = days / TRADING_DAYS
    z = rng.standard_normal(n)
    st = s0 * np.exp((mu - 0.5 * sigma**2) * t + sigma * np.sqrt(t) * z)
    return (s0 - st).astype(np.float32)


def run_batch(msg, keepalive):
    """Procesa un lote completo. `keepalive` se llama mientras espera, para no perder el heartbeat de AMQP."""
    job, b, n = msg["job_id"], msg["batch"], msg["n"]
    if FAIL_RATE and random.random() < FAIL_RATE:
        raise RuntimeError("falla inyectada")

    sizes = [CHUNK] * (n // CHUNK) + ([n % CHUNK] if n % CHUNK else [])
    tasks = [(m, msg["s0"], msg["mu"], msg["sigma"], msg["horizon_days"], [msg["seed"], b, i])
             for i, m in enumerate(sizes)]
    res = pool.map_async(simulate_chunk, tasks)
    while not res.ready():
        keepalive()
        res.wait(0.25)
    losses = np.concatenate(res.get())

    pipe = r.pipeline()
    pipe.set(f"job:{job}:batch:{b}", losses.tobytes(), ex=TTL)
    pipe.sadd(f"job:{job}:done", b)
    pipe.expire(f"job:{job}:done", TTL)
    pipe.execute()

    # el que termina el último lote registra el tiempo total de la solicitud
    done = r.scard(f"job:{job}:done")
    total = int(r.hget(f"job:{job}", "total_batches"))
    if done >= total and r.set(f"job:{job}:finished_at", time.time(), nx=True, ex=TTL):
        created = float(r.hget(f"job:{job}", "created_at"))
        JOB_TIME.observe(time.time() - created)
        JOBS_DONE.inc()
    return n


def on_message(ch, method, props, body):
    msg = json.loads(body)
    retries = (props.headers or {}).get("x-retries", 0)
    t0 = time.time()
    try:
        n = run_batch(msg, lambda: ch.connection.process_data_events(time_limit=0))
        SIMS.inc(n)
        BATCHES.labels("ok").inc()
        BATCH_TIME.observe(time.time() - t0)
    except pika.exceptions.AMQPError:
        raise                       # problema de conexión: que lo maneje el ciclo de reconexión
    except Exception as e:
        BATCHES.labels("error").inc()
        print(f"[{WORKER_ID}] error en lote {msg['job_id']}/{msg['batch']}: {e} (intento {retries + 1})", flush=True)
        if retries < MAX_RETRIES:
            time.sleep(0.5 * 2**retries)
            ch.basic_publish("", QUEUE, body, properties=pika.BasicProperties(
                delivery_mode=2, headers={"x-retries": retries + 1}))
        else:
            r.hset(f"job:{msg['job_id']}", "failed", 1)
            ch.basic_publish("", DLQ, body, properties=pika.BasicProperties(delivery_mode=2))
    ch.basic_ack(method.delivery_tag)


def heartbeat_loop():
    while True:
        try:
            r.set(f"worker:{WORKER_ID}:hb", time.time(), ex=HB_TTL)
        except redis.RedisError:
            pass
        time.sleep(HB_EVERY)


def consume_forever():
    params = pika.ConnectionParameters(
        RABBIT_HOST, credentials=pika.PlainCredentials(RABBIT_USER, RABBIT_PASS), heartbeat=30)
    while True:
        try:
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            ch.queue_declare(QUEUE, durable=True)
            ch.queue_declare(DLQ, durable=True)
            ch.basic_qos(prefetch_count=1)      # un lote a la vez: balanceo por disponibilidad
            ch.basic_consume(QUEUE, on_message)
            print(f"[{WORKER_ID}] listo con {PROCS} procesos", flush=True)
            ch.start_consuming()
        except (pika.exceptions.AMQPError, OSError) as e:
            print(f"[{WORKER_ID}] sin conexión a RabbitMQ ({e}); reintentando en 3 s", flush=True)
            time.sleep(3)


if __name__ == "__main__":
    pool = Pool(PROCS)                  # primero el pool, antes de crear hilos
    REGISTRY.register(ProcCollector())
    start_http_server(8001)
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    consume_forever()
