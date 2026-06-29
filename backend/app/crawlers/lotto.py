import logging
import re
from datetime import datetime

import httpx

from app.core.database import get_pool
from app.crawlers.common import (
    BASE_URL, client_session, get_client, run_bulk, run_retry,
)

logger = logging.getLogger(__name__)

_TASK_NAME = "crawl_lotto"

_OPT_VAL_RE = re.compile(r'id="opt_val"[^>]*value="(\d+)"')


async def fetch_latest_lotto_round() -> int:
    """동행복권 결과 페이지에서 최신 회차 번호 파싱"""
    async with client_session() as client:
        resp = await client.get(f"{BASE_URL}/lt645/result")
        resp.raise_for_status()
        m = _OPT_VAL_RE.search(resp.text)
        if not m:
            raise RuntimeError("[LOTTO] 최신 회차 파싱 실패: opt_val 못 찾음")
        latest = int(m.group(1))
        logger.info(f"[LOTTO] 최신 회차 감지: {latest}")
        return latest


async def crawl_lotto_round(
    round_no: int, client: httpx.AsyncClient | None = None
) -> list[dict]:
    c = client or await get_client()

    resp = await c.get(f"{BASE_URL}/lt645/selectPstLt645InfoNew.do", params={
        "srchDir": "center",
        "srchLtEpsd": round_no,
    })
    resp.raise_for_status()
    items = resp.json().get("data", {}).get("list", []) or []

    results: dict[int, dict] = {}
    for it in items:
        rn = it.get("ltEpsd")
        if not rn or rn in results:
            continue
        try:
            nums = sorted([
                it["tm1WnNo"], it["tm2WnNo"], it["tm3WnNo"],
                it["tm4WnNo"], it["tm5WnNo"], it["tm6WnNo"],
            ])
            results[rn] = {
                "round_no": rn,
                "draw_date": datetime.strptime(it["ltRflYmd"], "%Y%m%d").date(),
                "num1": nums[0], "num2": nums[1], "num3": nums[2],
                "num4": nums[3], "num5": nums[4], "num6": nums[5],
                "bonus": it["bnsWnNo"],
                "first_prize_amount": it.get("rnk1WnAmt") or 0,
                "first_prize_winners": it.get("rnk1WnNope") or 0,
                "total_sales": it.get("rlvtEpsdSumNtslAmt") or 0,
            }
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"[LOTTO] round={rn} 파싱 실패: {e}")

    logger.info(f"[LOTTO] srchLtEpsd={round_no}: {len(results)}회차 파싱")
    return list(results.values())


async def save_lotto_results_to_db(results: list[dict]) -> int:
    if not results:
        return 0

    pool = await get_pool()
    query = """
        INSERT INTO lotto_results (
            round_no, draw_date,
            num1, num2, num3, num4, num5, num6, bonus,
            first_prize_amount, first_prize_winners, total_sales
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
        )
        ON CONFLICT (round_no) DO NOTHING
    """
    rows = [
        (
            r["round_no"], r["draw_date"],
            r["num1"], r["num2"], r["num3"], r["num4"], r["num5"], r["num6"],
            r["bonus"],
            r["first_prize_amount"], r["first_prize_winners"], r["total_sales"],
        )
        for r in results
    ]

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(query, rows)

    logger.info(f"[DB] 로또 {len(rows)}건 저장 시도 (중복 제외)")
    return len(rows)


async def find_missing_lotto_rounds(latest_round: int, start_round: int = 1) -> list[int]:
    """누락 회차 조회"""
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT round_no FROM lotto_results WHERE round_no BETWEEN $1 AND $2",
        start_round, latest_round,
    )
    existing = {r["round_no"] for r in rows}
    expected = set(range(start_round, latest_round + 1))
    return sorted(expected - existing)


async def crawl_and_save_all_lotto_results(
    latest_round: int, start_round: int = 1
) -> dict:
    """초기 1회 전체 회차 적재"""
    logger.info(f"[START] crawl_lotto range={start_round}~{latest_round}")

    calls = list(range(start_round + 9, latest_round, 10))
    calls.append(latest_round)

    async def _one(client: httpx.AsyncClient, n: int) -> int:
        results = await crawl_lotto_round(n, client=client)
        return await save_lotto_results_to_db(results) if results else 0

    result = await run_bulk(_TASK_NAME, calls, _one)
    result["missing"] = await find_missing_lotto_rounds(latest_round, start_round)
    logger.info(
        f"[END] crawl_lotto: saved={result['saved']}, "
        f"failures={len(result['failures'])}, missing={len(result['missing'])}"
    )
    return result


async def retry_lotto_sub_keys(sub_keys: list[str]) -> dict:
    """실패 회차 재시도"""
    async def _one(client: httpx.AsyncClient, sub_key: str) -> None:
        n = int(sub_key)
        results = await crawl_lotto_round(n, client=client)
        if results:
            await save_lotto_results_to_db(results)

    return await run_retry(_TASK_NAME, sub_keys, _one)


async def crawl_latest_lotto_round() -> dict:
    """주간 스케줄 — 신규 회차만 저장"""
    pool = await get_pool()
    row = await pool.fetchrow("SELECT MAX(round_no) AS max_round FROM lotto_results")
    last_round = row["max_round"] or 0
    target = last_round + 1
    logger.info(f"[START] crawl_lotto_latest: last={last_round}, target={target}")

    async with client_session() as client:
        results = await crawl_lotto_round(target, client=client)

    saved = await save_lotto_results_to_db(results) if results else 0
    logger.info(f"[END] crawl_lotto_latest: saved={saved}")
    return {"saved": saved, "target": target}
