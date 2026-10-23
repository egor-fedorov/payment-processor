import json
import logging

import pytest

from app.core.logging import JsonFormatter, RequestLoggingMiddleware


def test_json_log_is_structured_and_preserves_exception():
    try:
        raise ValueError("failure")
    except ValueError:
        import sys

        record = logging.LogRecord(
            "test", logging.ERROR, "test.py", 1, "Payment %s", ("failed",), sys.exc_info()
        )
    record.payment_id = "payment-1"
    data = json.loads(JsonFormatter().format(record))
    assert data["message"] == "Payment failed"
    assert data["payment_id"] == "payment-1"
    assert "ValueError: failure" in data["exception"]


async def test_middleware_passes_lifespan_through():
    received = []

    async def app(scope, receive, send):
        received.append(scope)

    await RequestLoggingMiddleware(app)({"type": "lifespan"}, None, None)
    assert received == [{"type": "lifespan"}]


async def test_middleware_propagates_application_errors(caplog):
    async def fail(scope, receive, send):
        raise RuntimeError("failure")

    with caplog.at_level(logging.INFO), pytest.raises(RuntimeError):
        await RequestLoggingMiddleware(fail)(
            {"type": "http", "path": "/", "method": "GET"}, None, None
        )
    record = caplog.records[-1]
    assert record.status == 500
    assert record.request_id
