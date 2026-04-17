from pathlib import Path

from pydantic_settings import BaseSettings
import random

BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str

    DHLOTTERY_URL: str = "https://www.dhlottery.co.kr"
    CRAWL_REQUEST_DELAY: float = random.uniform(5, 10)

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


    class Config:
        env_file = str(BASE_DIR / ".env.dev")

settings = Settings()
