"""Reproducible local comparison; owns a unique Compose project and its volumes."""

import argparse
import asyncio
import json
import os
import platform
import socket
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic, perf_counter, time
from uuid import uuid4

import httpx
import psycopg

from app.core.config import Settings

ROOT = Path(__file__).resolve().parents[1]


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower), 3)


def free_ports(count: int) -> list[int]:
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


async def workload(api: str, receiver: str, count: int, key: str, db_url: str) -> dict:
    run_id = uuid4().hex
    headers = {"X-API-Key": key}
    semaphore = asyncio.Semaphore(16)
    latencies: list[float] = []
    sent: dict[str, float] = {}
    async with httpx.AsyncClient(timeout=10, headers=headers) as client:
        (await client.get(f"{api}/health/ready")).raise_for_status()

        async def create(index: int) -> None:
            async with semaphore:
                started = perf_counter()
                wall = time()
                response = await client.post(
                    f"{api}/api/v1/payments",
                    headers={"Idempotency-Key": f"{run_id}:{index}"},
                    json={
                        "amount": "10.00",
                        "currency": "USD",
                        "description": "benchmark",
                        "metadata": {"run": run_id},
                        "webhook_url": f"http://receiver:8000/hook/{run_id}",
                    },
                )
                response.raise_for_status()
                assert response.status_code == 202, response.text
                sent[response.json()["payment_id"]] = wall
                latencies.append((perf_counter() - started) * 1000)

        started = monotonic()
        await asyncio.gather(*(create(index) for index in range(count)))
        deadline = started + max(60, count * 6)
        while monotonic() < deadline:
            response = await client.get(f"{receiver}/deliveries/{run_id}")
            response.raise_for_status()
            received = response.json()
            if len(received) == count:
                break
            await asyncio.sleep(0.1)
        else:
            raise RuntimeError(f"Only {len(received)}/{count} callbacks arrived")
        duration = monotonic() - started
        # Callback receipt precedes the worker's commit. Require both delivery
        # records and a drained outbox before accepting the sample.
        connection = await psycopg.AsyncConnection.connect(db_url, autocommit=True)
        async with connection:
            while monotonic() < deadline:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        "SELECT count(*) FILTER (WHERE webhook_delivered_at IS NOT NULL), coalesce(sum(webhook_attempts), 0) FROM payments WHERE metadata->>'run' = %s",
                        (run_id,),
                    )
                    delivered, attempts = await cursor.fetchone()
                    await cursor.execute("SELECT count(*) FROM outbox WHERE published_at IS NULL")
                    (backlog,) = await cursor.fetchone()
                if delivered == count and backlog == 0:
                    break
                await asyncio.sleep(0.1)
            else:
                raise RuntimeError(f"Incomplete state: delivered={delivered}, backlog={backlog}")
        end_to_end = [(received[payment_id] - start) * 1000 for payment_id, start in sent.items()]
        return {
            "payments": count,
            "duration_seconds": round(duration, 3),
            "throughput_payments_per_second": round(count / duration, 3),
            "post_latency_ms": {
                f"p{int(p * 100)}": percentile(latencies, p) for p in (0.5, 0.95, 0.99)
            },
            "end_to_end_ms": {
                f"p{int(p * 100)}": percentile(end_to_end, p) for p in (0.5, 0.95, 0.99)
            },
            "webhook_attempts": int(attempts),
            "outbox_backlog_after": backlog,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--payments", type=int, default=40)
    parser.add_argument("--replicas", type=int, nargs="+", default=[1, 2])
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--gateway-min", type=int, default=2)
    parser.add_argument("--gateway-max", type=int, default=5)
    parser.add_argument("--output", type=Path, default=ROOT / "reports/benchmark.json")
    args = parser.parse_args()
    if (
        min(args.payments, args.concurrency, *args.replicas) < 1
        or not 0 <= args.gateway_min <= args.gateway_max < 25
    ):
        parser.error("Counts must be positive; gateway interval must satisfy 0 <= min <= max < 25")
    api_port, receiver_port, pg_port, amqp_port, management_port = free_ports(5)
    project = f"payment-bench-{uuid4().hex[:10]}"
    env = {
        **{
            key: value
            for key, value in os.environ.items()
            if key not in {name.upper() for name in Settings.model_fields}
        },
        "API_PORT": str(api_port),
        "RECEIVER_PORT": str(receiver_port),
        "POSTGRES_PORT": str(pg_port),
        "AMQP_PORT": str(amqp_port),
        "RABBITMQ_MANAGEMENT_PORT": str(management_port),
        "API_KEY": "benchmark-only",
        "CONSUMER_CONCURRENCY": str(args.concurrency),
        "PAYMENT_DELAY_MIN_SECONDS": str(args.gateway_min),
        "PAYMENT_DELAY_MAX_SECONDS": str(args.gateway_max),
        "OUTBOX_POLL_INTERVAL_SECONDS": "0.05",
        "PAYMENT_SUCCESS_RATE": "0.9",
        "WEBHOOK_LEASE_SECONDS": "30",
        "WEBHOOK_TIMEOUT_SECONDS": "5",
    }
    command = [
        "docker",
        "compose",
        "--env-file",
        str(ROOT / ".env.example"),
        "-p",
        project,
        "-f",
        str(ROOT / "docker-compose.yml"),
        "-f",
        str(ROOT / "docker-compose.benchmark.yml"),
    ]

    def compose(*arguments: str) -> None:
        subprocess.run([*command, *arguments], cwd=ROOT, env=env, check=True, timeout=180)

    report = {
        "timestamp": datetime.now(UTC).isoformat(),
        "platform": platform.platform(),
        "logical_cpus": os.cpu_count(),
        "python": platform.python_version(),
        "consumer_concurrency": args.concurrency,
        "gateway_delay_seconds": [args.gateway_min, args.gateway_max],
        "runs": [],
    }
    try:
        for replicas in args.replicas:
            compose("up", "-d", "--no-build", "--wait", "--scale", f"consumer={replicas}")
            values = (f"http://127.0.0.1:{api_port}", f"http://127.0.0.1:{receiver_port}")
            db_url = f"postgresql://payment:payment@127.0.0.1:{pg_port}/payments"
            asyncio.run(workload(*values, min(4, args.payments), "benchmark-only", db_url))
            result = asyncio.run(workload(*values, args.payments, "benchmark-only", db_url))
            result["consumer_replicas"] = replicas
            report["runs"].append(result)
            print(json.dumps(result, indent=2), flush=True)
    except BaseException:
        compose("logs", "--tail", "80")
        raise
    finally:
        compose("down", "--volumes", "--remove-orphans")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Report: {args.output}")


if __name__ == "__main__":
    main()
