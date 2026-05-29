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
    '''초기 GET으로 세션 쿠키를 받아둔 AsyncClient 생성'''
    client = httpx.AsyncClient(headers=HEADERS, timeout=15, follow_redirects=True)
    try:
        await client.get(BASE_URL)
    except httpx.HTTPError as e:
        logger.warning(f"[get_client] 초기 세션 GET 실패, 무시하고 진행: {e}")
    return client


async def delay(lo: int = 5, hi: int = 10) -> None:
    '''5~10초 딜레이 생성'''
    await asyncio.sleep(random.randint(lo, hi))


async def insert_bootstrap_failure(task_name: str, sub_key: str) -> None:
    """실패 기록. UPSERT이며, 과거 resolved된 행은 resolved_at을 NULL로 되돌림."""
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO bootstrap_failures (task_name, sub_key, failed_at, resolved_at)
        VALUES ($1, $2, NOW(), NULL)
        ON CONFLICT (task_name, sub_key) DO UPDATE SET
            failed_at   = NOW(),
            resolved_at = NULL
        """,
        task_name, sub_key,
    )


async def resolve_bootstrap_failure(task_name: str, sub_key: str) -> None:
    """실패 해결됨으로 수정"""
    pool = await get_pool()
    await pool.execute(
        """
        UPDATE bootstrap_failures SET resolved_at = NOW()
        WHERE task_name = $1 AND sub_key = $2 AND resolved_at IS NULL
        """,
        task_name, sub_key,
    )


async def get_pending_bootstrap_failures(task_name: str) -> list[str]:
    """재시도 대상 리스트 기져오기"""
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT sub_key FROM bootstrap_failures
        WHERE task_name = $1 AND resolved_at IS NULL
        ORDER BY failed_at
        """,
        task_name,
    )
    return [r["sub_key"] for r in rows]