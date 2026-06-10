from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str

    DHLOTTERY_URL: str = "https://www.dhlottery.co.kr"

    JWT_SECRET: str
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    REFRESH_TOKEN_EXPIRE_DAYS: int = 14

    SMTP_HOST: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USER: str
    SMTP_PASSWORD: str
    SMTP_FROM_NAME: str = "복권지도"

    EMAIL_CODE_TTL_MINUTES: int = 5
    EMAIL_CODE_RESEND_COOLDOWN_SECONDS: int = 180
    EMAIL_CODE_MAX_ATTEMPTS: int = 5
    EMAIL_CODE_MAX_EXHAUSTED_PER_DAY: int = 3

    model_config = SettingsConfigDict(env_file=str(BASE_DIR / ".env"))

settings = Settings()
