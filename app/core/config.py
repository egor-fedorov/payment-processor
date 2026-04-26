from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "payment-processor"
    api_v1_prefix: str = "/api/v1"
    api_key: str = "super-secret-key"
    database_url: str = "postgresql+asyncpg://payment:payment@postgres:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"
    outbox_poll_interval_seconds: float = 1.0
    outbox_batch_size: int = 50
    payment_success_rate: float = 0.9
    payment_delay_min_seconds: int = 2
    payment_delay_max_seconds: int = 5
    retry_max_attempts: int = 3
    retry_base_delay_seconds: int = 1
    webhook_timeout_seconds: float = 5.0

    @property
    def sync_database_url(self) -> str:
        return self.database_url.replace("+asyncpg", "+psycopg", 1)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
