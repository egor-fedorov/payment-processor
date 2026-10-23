"""Disposable webhook receiver used only by the local benchmark/smoke test."""

from time import time

from fastapi import FastAPI

from app.contracts import PaymentWebhookPayload

app = FastAPI()
deliveries: dict[str, dict[str, float]] = {}


@app.post("/hook/{run_id}")
async def hook(run_id: str, payload: PaymentWebhookPayload) -> dict[str, str]:
    deliveries.setdefault(run_id, {}).setdefault(str(payload.payment_id), time())
    return {"status": "accepted"}


@app.get("/deliveries/{run_id}")
async def results(run_id: str) -> dict[str, float]:
    return deliveries.get(run_id, {})


@app.get("/health/live")
async def health() -> dict[str, str]:
    return {"status": "alive"}
