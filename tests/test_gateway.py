from uuid import uuid4

import pytest

from app.domain import PaymentStatus
from app.services.gateway import SimulatedGateway


@pytest.mark.parametrize("rate", [0, 0.5, 1])
async def test_gateway_outcome_is_terminal_and_stable_across_instances(settings, rate):
    settings = settings.model_copy(update={"payment_success_rate": rate})
    original, replacement = SimulatedGateway(settings), SimulatedGateway(settings)
    for _ in range(10):
        payment_id = uuid4()
        outcome = await original.process(payment_id)
        assert outcome in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED)
        assert await original.process(payment_id) == outcome
        assert await replacement.process(payment_id) == outcome
        if rate in (0, 1):
            assert outcome == (PaymentStatus.SUCCEEDED if rate else PaymentStatus.FAILED)
