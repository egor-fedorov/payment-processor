from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "payment-processor"
    api_v1_prefix: str = "/api/v1"
    api_key: str = Field(default="super-secret-key", min_length=1)
    docs_enabled: bool = False
    database_url: str = "postgresql+asyncpg://payment:payment@postgres:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"
    outbox_poll_interval_seconds: float = Field(default=1.0, gt=0, allow_inf_nan=False)
    outbox_batch_size: int = Field(default=50, gt=0)
    payment_success_rate: float = Field(default=0.9, ge=0, le=1)
    payment_delay_min_seconds: int = Field(default=2, ge=0)
    payment_delay_max_seconds: int = Field(default=5, ge=0)
    retry_max_attempts: int = Field(default=3, ge=1, le=10)
    retry_base_delay_seconds: int = Field(default=1, gt=0)
    webhook_timeout_seconds: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    webhook_lease_seconds: float = Field(default=30.0, gt=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def validate_intervals(self) -> "Settings":
        if self.payment_delay_min_seconds > self.payment_delay_max_seconds:
            raise ValueError("payment delay minimum must not exceed maximum")
        if self.webhook_lease_seconds <= self.webhook_timeout_seconds:
            raise ValueError("webhook lease must exceed the total webhook timeout")
        return self

    @property
    def sync_database_url(self) -> str:
        return self.database_url.replace("+asyncpg", "+psycopg", 1)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
