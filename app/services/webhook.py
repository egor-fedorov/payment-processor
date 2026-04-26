import httpx


class WebhookDeliveryError(Exception):
    """Raised when webhook delivery fails."""


class WebhookSender:
    def __init__(self, timeout_seconds: float) -> None:
        self._timeout = timeout_seconds

    async def send(self, url: str, payload: dict) -> None:
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(str(exc)) from exc
