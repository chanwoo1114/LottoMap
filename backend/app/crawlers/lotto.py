import logging
from datetime import datetime

import httpx

from app.core.database import get_pool
from app.crawlers.common import (
    BASE_URL, delay, get_client, log_crawl_start, log_crawl_finish,
)

logger = logging.getLogger(__name__)


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
    '''DB에서 [start_round, latest_round] 범위 중 누락된 회차 조회'''
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
) -> int:
    '''초기 1회 실행용. 1~latest_round 전체 백필, 6회차 간격 호출'''
    log_id = await log_crawl_start("crawl_lotto_all")
    logger.info(f"[START] crawl_lotto_all range={start_round}~{latest_round}")

    total = 0
    try:
        client = await get_client()
        try:
            calls = list(range(start_round + 5, latest_round + 1, 6))
            if not calls or calls[-1] != latest_round:
                calls.append(latest_round)

            for n in calls:
                try:
                    results = await crawl_lotto_round(n, client=client)
                    if results:
                        total += await save_lotto_results_to_db(results)
                except Exception as e:
                    logger.error(f"[FAIL] srchLtEpsd={n}: {e}")
                await delay()
        finally:
            await client.aclose()

        missing = await find_missing_lotto_rounds(latest_round, start_round)
        status = "partial" if missing else "success"
        msg = (
            f"range={start_round}~{latest_round}, saved={total}, "
            f"missing={len(missing)}"
            + (f" {missing}" if missing else "")
        )
        await log_crawl_finish(log_id, status, msg)
        logger.info(f"[END] crawl_lotto_all: {msg}")

        return total
    except Exception as e:
        await log_crawl_finish(log_id, "failed", str(e))
        logger.error(f"[FAIL] crawl_lotto_all: {e}")
        raise


async def fill_missing_lotto_rounds(
    latest_round: int, start_round: int = 1
) -> int:
    '''누락된 회차만 재크롤링. 초기 백필 후 로그 보고 재실행할 때 사용'''
    log_id = await log_crawl_start("crawl_lotto_fill")

    try:
        missing = await find_missing_lotto_rounds(latest_round, start_round)
        if not missing:
            msg = f"range={start_round}~{latest_round}, 누락 없음"
            await log_crawl_finish(log_id, "success", msg)
            logger.info(f"[END] crawl_lotto_fill: {msg}")
            return 0

        logger.info(f"[START] crawl_lotto_fill: 누락 {len(missing)}건 {missing}")

        # srchLtEpsd=N이 N-5~N 윈도우를 반환하므로 높은 쪽부터 호출하며 커버 범위 누적
        targets = sorted(missing, reverse=True)
        covered: set[int] = set()
        saved_total = 0

        client = await get_client()
        try:
            for n in targets:
                if n in covered:
                    continue
                try:
                    results = await crawl_lotto_round(n, client=client)
                    if results:
                        saved_total += await save_lotto_results_to_db(results)
                        covered.update(r["round_no"] for r in results)
                except Exception as e:
                    logger.error(f"[FAIL] srchLtEpsd={n}: {e}")
                await delay()
        finally:
            await client.aclose()

        still_missing = await find_missing_lotto_rounds(latest_round, start_round)
        filled = sorted(set(missing) - set(still_missing))
        status = "partial" if still_missing else "success"
        msg = (
            f"filled={len(filled)}, still_missing={len(still_missing)}"
            + (f" {still_missing}" if still_missing else "")
        )
        await log_crawl_finish(log_id, status, msg)
        logger.info(f"[END] crawl_lotto_fill: {msg}")

        return saved_total
    except Exception as e:
        await log_crawl_finish(log_id, "failed", str(e))
        logger.error(f"[FAIL] crawl_lotto_fill: {e}")
        raise


async def crawl_latest_lotto_round() -> int:
    '''주간 스케줄용. DB 최신 회차 다음 회차를 시도'''
    log_id = await log_crawl_start("crawl_lotto_latest")

    try:
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

        if not results:
            msg = f"last={last_round}, target={target}, 새 회차 없음"
            await log_crawl_finish(log_id, "success", msg)
            logger.info(f"[END] crawl_lotto_latest: {msg}")
            return 0

        saved = await save_lotto_results_to_db(results)
        new_rounds = sorted(r["round_no"] for r in results if r["round_no"] > last_round)
        status = "success" if target in new_rounds else "partial"
        msg = f"last={last_round}, target={target}, new={new_rounds}, saved={saved}"
        await log_crawl_finish(log_id, status, msg)
        logger.info(f"[END] crawl_lotto_latest: {msg}")

        return saved
    except Exception as e:
        await log_crawl_finish(log_id, "failed", str(e))
        logger.error(f"[FAIL] crawl_lotto_latest: {e}")
        raise