import os


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


RABBITMQ_URL = env("RABBITMQ_URL", "amqp://guest:guest@rabbitmq/")
DATABASE_URL = env(
    "DATABASE_URL",
    "postgresql://postgres:postgres@postgres:5432/audio_pipeline",
)
STORAGE_ROOT = env("STORAGE_ROOT", "/data/audio-jobs")

MAX_RETRIES = int(env("MAX_RETRIES", "2"))
RETRY_BASE_SECONDS = float(env("RETRY_BASE_SECONDS", "1"))
