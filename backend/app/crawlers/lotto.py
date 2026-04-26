import logging
from datetime import datetime

import httpx

from app.core.database import get_pool
from app.crawlers.common import (
    BASE_URL, delay, get_client,
    insert_bootstrap_failure, resolve_bootstrap_failure,
)

logger = logging.getLogger(__name__)

_TASK_NAME = "crawl_lotto"


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
    """DB에서 [start_round, latest_round] 범위 중 누락된 회차 조회"""
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
    """초기 1회 백필. {"saved": N, "failures": [sub_keys]} 반환."""
    logger.info(f"[START] crawl_lotto range={start_round}~{latest_round}")

    total = 0
    failures: list[str] = []
    client = await get_client()
    try:
        calls = list(range(start_round + 5, latest_round + 1, 6))
        if not calls or calls[-1] != latest_round:
            calls.append(latest_round)

        for n in calls:
            sub_key = str(n)
            try:
                results = await crawl_lotto_round(n, client=client)
                if results:
                    total += await save_lotto_results_to_db(results)
            except Exception as e:
                failures.append(sub_key)
                try:
                    await insert_bootstrap_failure(_TASK_NAME, sub_key)
                except Exception as db_e:
                    logger.warning(f"[FAIL-LOG] DB 기록 실패: {db_e}")
                logger.error(f"[FAIL] srchLtEpsd={n}: {e}")
            await delay()
    finally:
        await client.aclose()

    missing = await find_missing_lotto_rounds(latest_round, start_round)
    logger.info(
        f"[END] crawl_lotto: saved={total}, failures={len(failures)}, "
        f"missing={len(missing)}"
    )
    return {"saved": total, "failures": failures, "missing": missing}


async def retry_lotto_sub_keys(sub_keys: list[str]) -> dict:
    """주어진 sub_key들(회차 문자열) 재시도. {"resolved": [...], "still_failed": [...]} 반환."""
    if not sub_keys:
        return {"resolved": [], "still_failed": []}

    logger.info(f"[RETRY] lotto {len(sub_keys)}건 재시도")
    resolved: list[str] = []
    still_failed: list[str] = []

    client = await get_client()
    try:
        for sub_key in sub_keys:
            try:
                n = int(sub_key)
            except ValueError:
                logger.warning(f"[RETRY] lotto sub_key 파싱 실패: {sub_key}")
                await insert_bootstrap_failure(_TASK_NAME, sub_key)
                still_failed.append(sub_key)
                continue
            try:
                results = await crawl_lotto_round(n, client=client)
                if results:
                    await save_lotto_results_to_db(results)
                await resolve_bootstrap_failure(_TASK_NAME, sub_key)
                resolved.append(sub_key)
            except Exception as e:
                await insert_bootstrap_failure(_TASK_NAME, sub_key)
                still_failed.append(sub_key)
                logger.warning(f"[RETRY] lotto {sub_key} 여전히 실패: {e}")
            await delay()
    finally:
        await client.aclose()

    logger.info(
        f"[RETRY] lotto: resolved={len(resolved)}, still_failed={len(still_failed)}"
    )
    return {"resolved": resolved, "still_failed": still_failed}


async def crawl_latest_lotto_round() -> dict:
    """주간 스케줄용. 최신 회차 다음 시도. {"saved": N, "target": round_no} 반환.
    실패 시 예외 raise (호출자가 worker_status 등 처리)."""
    pool = await get_pool()
    row = await pool.fetchrow("SELECT MAX(round_no) AS max_round FROM lotto_results")
    last_round = row["max_round"] or 0
    target = last_round + 1
    logger.info(f"[START] crawl_lotto_latest: last={last_round}, target={target}")

    client = await get_client()
    try:
        results = await crawl_lotto_round(target, client=client)
    finally:
        await client.aclose()

    saved = await save_lotto_results_to_db(results) if results else 0
    logger.info(f"[END] crawl_lotto_latest: saved={saved}")
    return {"saved": saved, "target": target}