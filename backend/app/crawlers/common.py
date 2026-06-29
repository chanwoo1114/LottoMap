import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

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


@asynccontextmanager
async def client_session():
    """세션 쿠키 받은 httpx 클라이언트를 열고 끝나면 자동으로 닫는다."""
    client = await get_client()
    try:
        yield client
    finally:
        await client.aclose()


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


async def run_bulk(
    task_name: str,
    keys: list,
    process: Callable[[httpx.AsyncClient, Any], Awaitable[int]],
    *,
    delay_lo: int = 5,
    delay_hi: int = 10,
) -> dict:
    """각 key를 process(client, key)로 처리(저장 건수 반환).
    예외 시 bootstrap_failures에 기록하고 다음 key로. 하나의 client_session을 공유하고
    key 사이에 delay. {"saved": N, "failures": [str(key), ...]}."""
    total = 0
    failures: list[str] = []
    async with client_session() as client:
        for key in keys:
            sub_key = str(key)
            try:
                total += await process(client, key)
            except Exception as e:
                failures.append(sub_key)
                try:
                    await insert_bootstrap_failure(task_name, sub_key)
                except Exception as db_e:
                    logger.warning(f"[FAIL-LOG] {sub_key} DB 기록 실패: {db_e}")
                logger.error(f"[FAIL] {task_name} {sub_key}: {e}")
            await delay(delay_lo, delay_hi)
    return {"saved": total, "failures": failures}


async def run_retry(
    task_name: str,
    sub_keys: list[str],
    process: Callable[[httpx.AsyncClient, str], Awaitable[None]],
    *,
    delay_lo: int = 5,
    delay_hi: int = 10,
) -> dict:
    """실패 sub_key 재시도. process(client, sub_key)가 fetch+save 수행(예외 시 실패).
    성공→resolve, 실패→재기록. {"resolved": [...], "still_failed": [...]}."""
    if not sub_keys:
        return {"resolved": [], "still_failed": []}

    logger.info(f"[RETRY] {task_name} {len(sub_keys)}건")
    resolved: list[str] = []
    still_failed: list[str] = []
    async with client_session() as client:
        for sub_key in sub_keys:
            try:
                await process(client, sub_key)
                await resolve_bootstrap_failure(task_name, sub_key)
                resolved.append(sub_key)
            except Exception as e:
                await insert_bootstrap_failure(task_name, sub_key)
                still_failed.append(sub_key)
                logger.warning(f"[RETRY] {task_name} {sub_key} 여전히 실패: {e}")
            await delay(delay_lo, delay_hi)
    logger.info(
        f"[RETRY] {task_name}: resolved={len(resolved)}, still_failed={len(still_failed)}"
    )
    return {"resolved": resolved, "still_failed": still_failed}