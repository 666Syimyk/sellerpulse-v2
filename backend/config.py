from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "sqlite:///./sellerpulse.db"
    secret_key: str = "change-me-in-production"
    token_encryption_key: str = ""
    access_token_expire_minutes: int = 60 * 24 * 7
    frontend_origin: str = "http://localhost:5173"
    sentry_dsn: str = ""
    trial_days: int = 14
    admin_secret: str = "change-admin-secret"
    enable_scheduler: bool = True
    sync_worker_poll_seconds: int = 5

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


@lru_cache
def get_settings() -> Settings:
    return Settings()
