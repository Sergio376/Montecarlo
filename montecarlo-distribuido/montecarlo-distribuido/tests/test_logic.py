"""Pruebas de la lógica (sin Docker): API + worker sobre un Redis falso y sin RabbitMQ.

Correr desde la raíz del proyecto:  pytest -q tests
"""
import json
import os
import sys
from multiprocessing.pool import ThreadPool

import fakeredis
import pytest
from fastapi.testclient import TestClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [os.path.join(ROOT, "api"), os.path.join(ROOT, "worker")]
import main as api      # noqa: E402
import worker           # noqa: E402


class FakePublisher:
    def __init__(self):
        self.bodies = []

    def publish_many(self, bodies):
        self.bodies.extend(bodies)


@pytest.fixture()
def env():
    fake = fakeredis.FakeRedis()
    api.r = fake
    worker.r = fake
    api.publisher = FakePublisher()
    worker.pool = ThreadPool(3)
    yield TestClient(api.app), api.publisher
    worker.pool.close()


def correr_lotes(bodies, orden=None):
    for body in (orden(bodies) if orden else bodies):
        worker.run_batch(json.loads(body), lambda: None)


def lanzar(client, **extra):
    payload = {"n_simulations": 200_000, "batch_size": 50_000, "seed": 42, **extra}
    resp = client.post("/simulate", json=payload)
    assert resp.status_code == 202
    return resp.json()["job_id"]


def test_se_divide_en_lotes(env):
    client, pub = env
    lanzar(client, n_simulations=120_000)
    assert len(pub.bodies) == 3
    assert sum(json.loads(b)["n"] for b in pub.bodies) == 120_000


def test_var_cercano_al_analitico(env):
    client, pub = env
    job = lanzar(client, n_simulations=800_000)
    correr_lotes(pub.bodies)
    data = client.get(f"/jobs/{job}").json()
    assert data["status"] == "done"
    res = data["result"]
    assert abs(res["var"] - res["var_analytic"]) / res["var_analytic"] < 0.03
    assert res["expected_shortfall"] >= res["var"]


def test_progreso_parcial(env):
    client, pub = env
    job = lanzar(client)
    assert client.get(f"/jobs/{job}").json()["status"] == "queued"
    correr_lotes(pub.bodies[:2])
    data = client.get(f"/jobs/{job}").json()
    assert data["status"] == "running" and data["batches_done"] == 2


def test_reentrega_es_idempotente(env):
    """Si un worker muere sin hacer ack, otro repite el lote y el resultado no cambia."""
    client, pub = env
    job = lanzar(client)
    correr_lotes(pub.bodies + pub.bodies[:2])      # dos lotes se procesan dos veces
    data = client.get(f"/jobs/{job}").json()
    assert data["status"] == "done" and data["batches_done"] == 4


def test_resultado_no_depende_del_orden_ni_de_los_procesos(env):
    client, pub = env
    j1 = lanzar(client)
    correr_lotes(pub.bodies, orden=lambda b: list(reversed(b)))
    v1 = client.get(f"/jobs/{j1}").json()["result"]["var"]

    pub.bodies.clear()
    worker.pool.close()
    worker.pool = ThreadPool(1)
    j2 = lanzar(client)
    correr_lotes(pub.bodies)
    v2 = client.get(f"/jobs/{j2}").json()["result"]["var"]
    assert v1 == v2


def test_workers_activos(env):
    client, _ = env
    api.r.set("worker:abc:hb", 1, ex=10)
    api.r.set("worker:def:hb", 1, ex=10)
    assert client.get("/workers").json()["active"] == 2
