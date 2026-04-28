import logging
from datetime import datetime

import httpx

from app.core.database import get_pool
from app.crawlers.common import (
    BASE_URL, get_client,
    insert_bootstrap_failure, resolve_bootstrap_failure,
)

logger = logging.getLogger(__name__)

_TASK_NAME = "crawl_pension"

_API_URL = f"{BASE_URL}/pt720/selectPstPt720WnList.do"
_API_HEADERS = {
    "AJAX": "true",
    "requestMenuUri": "/pt720/result",
    "Referer": f"{BASE_URL}/pt720/result",
}


async def fetch_all_pension_results(
    client: httpx.AsyncClient | None = None
) -> list[dict]:
    """연금복권 720+ 전체 회차를 단일 JSON API 호출로 가져온다"""
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
    """회차 dict 리스트 저장, 중복은 건너뜀"""
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


async def crawl_and_save_all_pension_results() -> dict:
    """초기 1회 백필. {"saved": N, "failures": [sub_keys]} 반환.
    단일 API 호출이라 sub_key는 'all' 단일."""
    logger.info("[START] crawl_pension_all")

    try:
        client = await get_client()
        try:
            results = await fetch_all_pension_results(client=client)
        finally:
            await client.aclose()

        saved = await save_pension_results_to_db(results)
        logger.info(f"[END] crawl_pension_all: fetched={len(results)}, saved={saved}")
        return {"saved": saved, "failures": []}
    except Exception as e:
        logger.exception(f"[FAIL] crawl_pension_all: {e}")
        try:
            await insert_bootstrap_failure(_TASK_NAME, "all")
        except Exception as db_e:
            logger.warning(f"[FAIL-LOG] DB 기록 실패: {db_e}")
        return {"saved": 0, "failures": ["all"]}


async def retry_pension_sub_keys(sub_keys: list[str]) -> dict:
    """pension은 단일 API라 sub_key='all'/'latest' 지원."""
    if not sub_keys:
        return {"resolved": [], "still_failed": []}

    logger.info(f"[RETRY] pension {sub_keys}")
    resolved: list[str] = []
    still_failed: list[str] = []

    for sub_key in sub_keys:
        try:
            if sub_key == "all":
                result = await crawl_and_save_all_pension_results()
                if not result["failures"]:
                    await resolve_bootstrap_failure(_TASK_NAME, sub_key)
                    resolved.append(sub_key)
                else:
                    await insert_bootstrap_failure(_TASK_NAME, sub_key)
                    still_failed.append(sub_key)
            elif sub_key == "latest":
                await crawl_latest_pension_round()
                await resolve_bootstrap_failure(_TASK_NAME, sub_key)
                resolved.append(sub_key)
            else:
                logger.warning(f"[RETRY] pension 미지원 sub_key: {sub_key}")
                await insert_bootstrap_failure(_TASK_NAME, sub_key)
                still_failed.append(sub_key)
        except Exception as e:
            await insert_bootstrap_failure(_TASK_NAME, sub_key)
            still_failed.append(sub_key)
            logger.warning(f"[RETRY] pension {sub_key} 여전히 실패: {e}")

    return {"resolved": resolved, "still_failed": still_failed}


async def crawl_latest_pension_round() -> dict:
    """주간 스케줄용. DB 최신 회차 이후만 저장. {"saved": N, "new_rounds": [...]} 반환.
    실패 시 예외 raise."""
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
        logger.info(f"[END] crawl_pension_latest: last={last_round}, 새 회차 없음")
        return {"saved": 0, "new_rounds": []}

    saved = await save_pension_results_to_db(new_results)
    new_rounds = sorted(r["round_no"] for r in new_results)
    logger.info(
        f"[END] crawl_pension_latest: last={last_round}, new={new_rounds}, saved={saved}"
    )
    return {"saved": saved, "new_rounds": new_rounds}