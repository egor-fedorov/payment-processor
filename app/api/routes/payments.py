from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import verify_api_key
from app.api.schemas import (
    PaymentAcceptedResponse,
    PaymentCreateRequest,
    PaymentDetailsResponse,
)
from app.db.session import get_session
from app.services.payments import PaymentService

router = APIRouter(
    prefix="/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
)


@router.post("", response_model=PaymentAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_payment(
    payload: PaymentCreateRequest,
    session: AsyncSession = Depends(get_session),
    idempotency_key: str = Header(
        ..., alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"\S"
    ),
) -> PaymentAcceptedResponse:
    payment = await PaymentService(session).create_payment(payload, idempotency_key)
    return PaymentAcceptedResponse.from_payment(payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentDetailsResponse,
    status_code=status.HTTP_200_OK,
)
async def get_payment(
    payment_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PaymentDetailsResponse:
    payment = await PaymentService(session).get_payment(payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentDetailsResponse.from_payment(payment)
