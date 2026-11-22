# Plataforma distribuida de simulación Monte Carlo (VaR)

Proyecto de Sistemas Distribuidos. Recibe solicitudes para calcular el Valor en Riesgo (VaR) de un
portafolio, reparte el trabajo entre nodos trabajadores que simulan en paralelo y sigue funcionando
si se cae un nodo.

## Arquitectura

```
cliente -> nginx -> API (FastAPI) --lotes--> RabbitMQ --> worker 1..N (multiprocessing)
                        |                                      |
                        +---------- Redis (primario + réplica) +   (estado y resultados parciales)

Prometheus <- API, workers, RabbitMQ  ->  Grafana
```

| Servicio | Para qué |
|---|---|
| `nginx` | Entrada única (`localhost:8080`), balancea entre réplicas de la API |
| `api` | REST: recibe solicitudes, las parte en lotes y junta los resultados |
| `rabbitmq` | Cola `mc_tasks`; los workers consumen con `prefetch=1` y ack manual |
| `worker` | Simula lotes con un pool de procesos y guarda el resultado en Redis |
| `redis-primary` / `redis-replica` | Estado de cada solicitud, con réplica |
| `prometheus` / `grafana` | Métricas y dashboard |

## Cómo correrlo

Requisitos: Docker con Compose v2.

```bash
docker compose up -d --build
```

- API: http://localhost:8080/docs
- Grafana: http://localhost:3000 (admin / admin), dashboard **Monte Carlo distribuido**
- Prometheus: http://localhost:9090
- RabbitMQ: http://localhost:15672 (mc / mcpass)

Por defecto levanta 2 workers. Para cambiar el número:

```bash
docker compose up -d --scale worker=4
```

Cada worker tiene `cpus: 2` y 2 procesos (variables `WORKER_CPUS` y `WORKER_PROCESSES`). Si tu máquina
tiene pocos núcleos, bájalos (por ejemplo `WORKER_CPUS=1 WORKER_PROCESSES=1`), porque si los workers
compiten por los mismos núcleos no vas a ver escalamiento.

## Probar una solicitud

```bash
curl -s -X POST localhost:8080/simulate -H "Content-Type: application/json" \
  -d '{"n_simulations": 1000000, "batch_size": 50000, "horizon_days": 10, "confidence": 0.99}'
# -> {"job_id":"a1b2c3...","batches":20,...}

curl -s localhost:8080/jobs/<job_id>     # queued / running / done (con var, expected_shortfall, var_analytic)
curl -s localhost:8080/workers           # workers con heartbeat vigente
```

El resultado trae `var_analytic`, el VaR exacto para este modelo, para comparar contra la simulación
(sale en la validación del reporte).

## Pruebas

**Lógica (sin Docker):**

```bash
pip install -r tests/requirements.txt
pytest -q tests
```

**Carga y escalabilidad (1, 2 y 4 workers):**

```bash
pip install -r loadtest/requirements.txt
docker compose up -d --scale worker=1
locust -f loadtest/locustfile.py --headless -u 8 -r 2 -t 2m --host http://localhost:8080 --csv results/w1
# repetir con --scale worker=2 y 4 (results/w2, results/w4)
```

En los CSV, la fila `JOB end_to_end` trae throughput (requests/s) y la latencia p95 de punta a punta.

**Inyección de fallas:**

1. Con 4 workers y una prueba de carga corriendo, apaga uno: `docker stop montecarlo-worker-2`
   (o `docker kill` para una caída abrupta).
2. Observa en Grafana: baja "Workers activos", sube un momento la cola y el throughput se recupera.
3. El lote que tenía ese worker sin confirmar regresa a la cola y lo toma otro. Para el reporte mide
   cuánto tarda en detectarse (el heartbeat expira a los 10 s) y en recuperarse el throughput.
4. Para probar reintentos: `FAIL_RATE=0.2 docker compose up -d --scale worker=2` hace fallar al azar el 20 %
   de los lotes; se reintentan hasta 3 veces y después pasan a `mc_tasks.dead`.

## Decisiones de diseño (para el reporte)

- **Resultados deterministas.** La semilla de cada tarea es (semilla de la solicitud, lote, chunk), no
  depende de qué worker la procese. Un lote repetido da exactamente el mismo resultado, y por eso la
  reentrega al caerse un worker es segura (idempotencia).
- **Entrega al menos una vez.** Ack manual en RabbitMQ: puede haber lotes repetidos, nunca perdidos.
  Los resultados parciales se guardan con clave fija y un `SADD` por lote, así que un duplicado no cuenta doble.
- **Heartbeats.** Cada worker renueva `worker:<id>:hb` en Redis cada 3 s con TTL de 10 s; la API
  y Grafana cuentan los nodos vivos con esas claves. Aparte, AMQP tiene su propio heartbeat de 30 s.
- **CAP.** El estado de las solicitudes está en un Redis primario con réplica asíncrona. Si el primario
  se cae no hay failover automático (ver pendientes); en la cola se prioriza disponibilidad.

## Pendientes (semanas 13 a 15)

- [ ] Diagrama de arquitectura y de despliegue (para el avance)
- [ ] Redis Sentinel para failover automático del primario (hoy la réplica solo es respaldo)
- [ ] Alerta si una solicitud pasa de cierto tiempo sin avanzar (timeout de solicitud)
- [ ] Correr y documentar las pruebas de carga con 1, 2 y 4 workers
- [ ] Correr y documentar la inyección de fallas
- [ ] (Opcional) Desplegar workers en máquinas distintas con Tailscale
- [ ] Reporte IEEE y video

`docker compose --profile cadvisor up -d` agrega cAdvisor (métricas por contenedor) si lo quieres; el
dashboard ya muestra CPU y memoria de cada worker sin él.
