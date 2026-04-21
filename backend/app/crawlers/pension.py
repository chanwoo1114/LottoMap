import logging
from datetime import datetime

import httpx

from app.core.database import get_pool
from app.crawlers.common import (
    BASE_URL, get_client, log_crawl_start, log_crawl_finish,
)

logger = logging.getLogger(__name__)

_API_URL = f"{BASE_URL}/pt720/selectPstPt720WnList.do"
_API_HEADERS = {
    "AJAX": "true",
    "requestMenuUri": "/pt720/result",
    "Referer": f"{BASE_URL}/pt720/result",
}


async def fetch_all_pension_results(
    client: httpx.AsyncClient | None = None
) -> list[dict]:
    '''연금복권 720+ 전체 회차를 단일 JSON API 호출로 가져온다'''
    c = client or await get_client()

    resp = await c.get(_API_URL, headers=_API_HEADERS)
    resp.raise_for_status()
    items = resp.json().get("data", {}).get("result", []) or []

    results: list[dict] = []
    for it in items:
        try:
            results.append({
                "round_no": int(it["psltEpsd"]),
                "draw_date": datetime.strptime(it["psltRflYmd"], "%Y%m%d").date(),
                "first_prize_group": int(it["wnBndNo"]),
                "first_prize_number": it["wnRnkVl"],
                "bonus_number": it["bnsRnkVl"],
            })
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(
                f"[PENSION] 파싱 실패 psltEpsd={it.get('psltEpsd')}: {e}"
            )

    logger.info(f"[PENSION] {len(results)}회차 파싱")
    return results


async def save_pension_results_to_db(results: list[dict]) -> int:
    '''회차 dict 리스트 저장, 중복은 건너뜀'''
    if not results:
        return 0

    pool = await get_pool()
    query = """
        INSERT INTO pension_results (
            round_no, draw_date,
            first_prize_group, first_prize_number, bonus_number
        ) VALUES (
            $1, $2, $3, $4, $5
        )
        ON CONFLICT (round_no) DO NOTHING
    """
    rows = [
        (
            r["round_no"], r["draw_date"],
            r["first_prize_group"], r["first_prize_number"], r["bonus_number"],
        )
        for r in results
    ]

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(query, rows)

    logger.info(f"[DB] 연금복권 {len(rows)}건 저장 시도 (중복 제외)")
    return len(rows)


async def crawl_and_save_all_pension_results() -> int:
    '''초기 1회 실행용. 전체 회차를 단일 요청으로 백필'''
    log_id = await log_crawl_start("crawl_pension_all")
    logger.info("[START] crawl_pension_all")

    try:
        client = await get_client()
        try:
            results = await fetch_all_pension_results(client=client)
        finally:
            await client.aclose()

        saved = await save_pension_results_to_db(results)
        msg = f"fetched={len(results)}, saved={saved}"
        await log_crawl_finish(log_id, "success", msg)
        logger.info(f"[END] crawl_pension_all: {msg}")

        return saved
    except Exception as e:
        await log_crawl_finish(log_id, "failed", str(e))
        logger.error(f"[FAIL] crawl_pension_all: {e}")
        raise


async def crawl_latest_pension_round() -> int:
    '''주간 스케줄용. 전체 응답 중 DB 최신 회차 이후만 저장 (매주 목요일 추첨)'''
    log_id = await log_crawl_start("crawl_pension_latest")

    try:
        pool = await get_pool()
        row = await pool.fetchrow(
            "SELECT MAX(round_no) AS max_round FROM pension_results"
        )
        last_round = row["max_round"] or 0

        client = await get_client()
        try:
            results = await fetch_all_pension_results(client=client)
        finally:
            await client.aclose()

        new_results = [r for r in results if r["round_no"] > last_round]
        if not new_results:
            msg = f"last={last_round}, 새 회차 없음"
            await log_crawl_finish(log_id, "partial", msg)
            logger.info(f"[END] crawl_pension_latest: {msg}")
            return 0

        saved = await save_pension_results_to_db(new_results)
        new_rounds = sorted(r["round_no"] for r in new_results)
        msg = f"last={last_round}, new={new_rounds}, saved={saved}"
        await log_crawl_finish(log_id, "success", msg)
        logger.info(f"[END] crawl_pension_latest: {msg}")

        return saved
    except Exception as e:
        await log_crawl_finish(log_id, "failed", str(e))
        logger.error(f"[FAIL] crawl_pension_latest: {e}")
        raise
