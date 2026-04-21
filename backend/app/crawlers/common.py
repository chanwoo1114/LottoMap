"""크롤러 공통 유틸리티 (HTTP 클라이언트, 딜레이, crawl_logs 기록)"""
import asyncio
import logging
import random

import httpx

from app.core.config import settings
from app.core.database import get_pool

logger = logging.getLogger(__name__)

BASE_URL = settings.DHLOTTERY_URL
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": f"{BASE_URL}/prchsplcsrch/home",
}


async def get_client() -> httpx.AsyncClient:
    client = httpx.AsyncClient(headers=HEADERS, timeout=15, follow_redirects=True)
    await client.get(BASE_URL)
    return client


async def delay(lo: int = 5, hi: int = 11) -> None:
    await asyncio.sleep(random.randint(lo, hi))


async def log_crawl_start(task_name: str) -> int:
    '''크롤 시작 시점에 crawl_logs에 running 상태로 row 생성, id 반환'''
    pool = await get_pool()
    row = await pool.fetchrow(
        "INSERT INTO crawl_logs (task_name) VALUES ($1) RETURNING id",
        task_name,
    )
    return row["id"]


async def log_crawl_finish(log_id: int, status: str, message: str = "") -> None:
    '''크롤 종료 시점에 status/message/finished_at 갱신'''
    pool = await get_pool()
    await pool.execute(
        """
        UPDATE crawl_logs
        SET status      = $1,
            message     = $2,
            finished_at = NOW()
        WHERE id = $3
        """,
        status, message, log_id,
    )