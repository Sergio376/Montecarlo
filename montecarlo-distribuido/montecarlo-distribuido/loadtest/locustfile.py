"""Prueba de carga: cada usuario manda una solicitud y espera hasta que termine.

Mide el tiempo de punta a punta como el evento "JOB end_to_end" (de ahí sale la latencia p95).
Ejemplo:
  locust -f loadtest/locustfile.py --headless -u 8 -r 2 -t 2m \
         --host http://localhost:8080 --csv results/w2
"""
import time

from locust import HttpUser, between, events, task

PAYLOAD = {"n_simulations": 2_000_000, "batch_size": 50_000, "horizon_days": 10, "confidence": 0.99}


class Usuario(HttpUser):
    wait_time = between(0.2, 0.5)

    @task
    def solicitud(self):
        t0 = time.time()
        with self.client.post("/simulate", json=PAYLOAD, name="POST /simulate", catch_response=True) as resp:
            if resp.status_code != 202:
                resp.failure(f"status {resp.status_code}")
                return
            job_id = resp.json()["job_id"]

        error = None
        while time.time() - t0 < 120:
            time.sleep(0.2)
            data = self.client.get(f"/jobs/{job_id}", name="GET /jobs/[id]").json()
            if data["status"] == "done":
                break
            if data["status"] == "failed":
                error = Exception("job failed")
                break
        else:
            error = Exception("timeout de 120 s")

        events.request.fire(request_type="JOB", name="end_to_end",
                            response_time=(time.time() - t0) * 1000, response_length=0,
                            exception=error, context={})
