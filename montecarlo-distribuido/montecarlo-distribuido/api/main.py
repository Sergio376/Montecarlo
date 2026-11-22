"""API REST: recibe solicitudes de simulación, las parte en lotes y las manda a la cola.

Flujo:
  POST /simulate  -> guarda la solicitud en Redis y publica N lotes en RabbitMQ
  GET  /jobs/{id} -> estado/progreso; cuando todos los lotes terminan, junta los
                     resultados parciales y calcula VaR y ES
"""
import json
import math
import os
import threading
import time
import uuid
from statistics import NormalDist

import numpy as np
import pika
import redis
from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, Field

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_USER = os.getenv("RABBIT_USER", "mc")
RABBIT_PASS = os.getenv("RABBIT_PASS", "mcpass")
REDIS_HOST = os.getenv("REDIS_HOST", "redis-primary")
QUEUE = "mc_tasks"
TTL = 3600            # los datos de cada solicitud viven 1 hora en Redis
TRADING_DAYS = 252

r = redis.Redis(host=REDIS_HOST, port=6379)

# ---------- Métricas ----------
JOBS_SUBMITTED = Counter("mc_jobs_submitted_total", "Solicitudes recibidas")
HTTP_TIME = Histogram(
    "http_request_duration_seconds", "Latencia de la API",
    ["method", "path", "status"], buckets=(.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5),
)
WORKERS_ACTIVE = Gauge("mc_workers_active", "Trabajadores con heartbeat vigente")


class SimRequest(BaseModel):
    s0: float = Field(1_000_000, gt=0, description="Valor inicial del portafolio")
    mu: float = Field(0.08, description="Rendimiento anual esperado")
    sigma: float = Field(0.20, gt=0, description="Volatilidad anual")
    horizon_days: int = Field(10, ge=1, le=365)
    n_simulations: int = Field(1_000_000, ge=1_000, le=100_000_000)
    batch_size: int = Field(50_000, ge=1_000)
    confidence: float = Field(0.99, gt=0.5, lt=1)
    seed: int | None = None


class Publisher:
    """Conexión a RabbitMQ con confirmaciones de publicación y reconexión automática."""

    def __init__(self):
        self._lock = threading.Lock()
        self._conn = None
        self._ch = None

    def _open(self):
        params = pika.ConnectionParameters(
            RABBIT_HOST,
            credentials=pika.PlainCredentials(RABBIT_USER, RABBIT_PASS),
            heartbeat=0,
            socket_timeout=5,
        )
        self._conn = pika.BlockingConnection(params)
        self._ch = self._conn.channel()
        self._ch.queue_declare(QUEUE, durable=True)
        self._ch.confirm_delivery()

    def publish_many(self, bodies):
        with self._lock:
            for attempt in (1, 2):
                try:
                    if self._conn is None or self._conn.is_closed:
                        self._open()
                    for body in bodies:
                        self._ch.basic_publish(
                            "", QUEUE, body,
                            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
                        )
                    return
                except (pika.exceptions.AMQPError, OSError):
                    self._conn = None
                    if attempt == 2:
                        raise


publisher = Publisher()
app = FastAPI(title="Monte Carlo distribuido")


@app.middleware("http")
async def timing(request: Request, call_next):
    t0 = time.perf_counter()
    response = await call_next(request)
    if request.url.path != "/metrics":
        route = request.scope.get("route")
        path = route.path if route else request.url.path
        HTTP_TIME.labels(request.method, path, str(response.status_code)).observe(time.perf_counter() - t0)
    return response


def active_workers():
    """Un trabajador está vivo si su heartbeat (clave con TTL de 10 s) sigue en Redis."""
    return sorted(k.decode().split(":")[1] for k in r.scan_iter("worker:*:hb"))


def _workers_gauge():
    try:
        return len(active_workers())
    except redis.RedisError:
        return 0


WORKERS_ACTIVE.set_function(_workers_gauge)


def analytic_var(s0, mu, sigma, days, conf):
    """VaR exacto bajo movimiento browniano geométrico; sirve para validar la simulación."""
    t = days / TRADING_DAYS
    z = NormalDist().inv_cdf(1 - conf)
    st = s0 * math.exp((mu - 0.5 * sigma**2) * t + sigma * math.sqrt(t) * z)
    return s0 - st


@app.post("/simulate", status_code=202)
def simulate(req: SimRequest):
    job_id = uuid.uuid4().hex[:12]
    n_batches = math.ceil(req.n_simulations / req.batch_size)
    seed = req.seed if req.seed is not None else int(time.time() * 1000) % 2**31

    # 1) guardar la solicitud (antes de publicar, para que el worker ya la encuentre)
    r.hset(f"job:{job_id}", mapping={
        "created_at": time.time(), "total_batches": n_batches, "n_simulations": req.n_simulations,
        "s0": req.s0, "mu": req.mu, "sigma": req.sigma, "horizon_days": req.horizon_days,
        "confidence": req.confidence, "seed": seed,
    })
    r.expire(f"job:{job_id}", TTL)

    # 2) partir en lotes y publicar
    bodies = []
    for b in range(n_batches):
        size = min(req.batch_size, req.n_simulations - b * req.batch_size)
        bodies.append(json.dumps({
            "job_id": job_id, "batch": b, "n": size, "s0": req.s0, "mu": req.mu,
            "sigma": req.sigma, "horizon_days": req.horizon_days, "seed": seed,
        }).encode())
    try:
        publisher.publish_many(bodies)
    except (pika.exceptions.AMQPError, OSError):
        raise HTTPException(503, "No se pudo publicar en la cola de mensajes")

    JOBS_SUBMITTED.inc()
    return {"job_id": job_id, "batches": n_batches, "status_url": f"/jobs/{job_id}"}


def aggregate(job_id, meta):
    """Junta las pérdidas de todos los lotes y calcula VaR y ES (pérdida esperada en la cola)."""
    cached = r.get(f"job:{job_id}:result")
    if cached:
        return json.loads(cached)
    parts = []
    for b in range(int(meta["total_batches"])):
        raw = r.get(f"job:{job_id}:batch:{b}")
        if raw is None:
            return None
        parts.append(np.frombuffer(raw, dtype=np.float32))
    losses = np.concatenate(parts)
    conf = float(meta["confidence"])
    var = float(np.percentile(losses, conf * 100))
    es = float(losses[losses >= var].mean())
    finished = r.get(f"job:{job_id}:finished_at")
    elapsed = (float(finished) if finished else time.time()) - float(meta["created_at"])
    result = {
        "confidence": conf,
        "var": round(var, 2),
        "expected_shortfall": round(es, 2),
        "var_analytic": round(analytic_var(float(meta["s0"]), float(meta["mu"]), float(meta["sigma"]),
                                           int(meta["horizon_days"]), conf), 2),
        "n_simulations": int(losses.size),
        "elapsed_seconds": round(elapsed, 3),
    }
    r.set(f"job:{job_id}:result", json.dumps(result), ex=TTL)
    # ya con el resultado guardado, se borran los lotes para no llenar la memoria de Redis
    r.delete(*[f"job:{job_id}:batch:{b}" for b in range(int(meta["total_batches"]))])
    return result


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    meta = {k.decode(): v.decode() for k, v in r.hgetall(f"job:{job_id}").items()}
    if not meta:
        raise HTTPException(404, "Solicitud no encontrada (o ya expiró)")
    total = int(meta["total_batches"])
    done = r.scard(f"job:{job_id}:done")
    base = {"job_id": job_id, "batches_done": done, "batches_total": total}
    if meta.get("failed"):
        return {**base, "status": "failed"}
    if done < total:
        return {**base, "status": "queued" if done == 0 else "running", "progress": round(done / total, 3)}
    result = aggregate(job_id, meta)
    if result is None:
        return {**base, "status": "running", "progress": round(done / total, 3)}
    return {**base, "status": "done", "progress": 1.0, "result": result}


@app.get("/workers")
def workers():
    w = active_workers()
    return {"active": len(w), "workers": w}


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
