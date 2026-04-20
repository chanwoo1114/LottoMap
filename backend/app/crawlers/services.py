import asyncio
import logging
import re
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

from app.core.config import settings
from app.core.database import get_pool
from app.crawlers.regions import SIDO_LIST, ALL_REGION_PAIRS, CTPV_MAP

logger = logging.getLogger(__name__)
BASE_URL = settings.DHLOTTERY_URL
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": f"{BASE_URL}/prchsplcsrch/home",
}

async def _get_client() -> httpx.AsyncClient:
    client = httpx.AsyncClient(headers=HEADERS, timeout=15, follow_redirects=True)
    await client.get(BASE_URL)
    return client


async def _delay():
    import random
    rnd = random.randint(5, 11)
    await asyncio.sleep(rnd)


# 판매점 위치
async def crawl_store_location_by_region(sido, sigungu, client: httpx.AsyncClient | None = None) -> list[dict]:
    c = client or await _get_client()

    stores, page = [], 1

    while True:
        try:
            resp = await c.get(f"{BASE_URL}/prchsplcsrch/selectLtShp.do", params={
                "srchCtpvNm": CTPV_MAP[sido],
                "srchSggNm": sigungu,
                "pageNum": page,
                "recordCountPerPage": 10,
                "pageCount": 5,
            })
            data = resp.json().get("data", {})
            if not data:
                break
            items = data.get("list", [])
            if not items:
                break

            for item in items:
                stores.append({
                    "store_id": item.get("ltShpId", ""),
                    "name": item.get("conmNm", ""),
                    "phone": item.get("shpTelno") or "",
                    "address": item.get("bplcRdnmDaddr", "").strip(),
                    "address_detail": item.get("bplcLctnDaddr") or "",
                    "sido": sido,
                    "sigungu": sigungu,
                    "dong": (item.get("tm3BplcLctnAddr") or "").strip(),
                    "lat": item.get("shpLat"),
                    "lon": item.get("shpLot"),
                    "sells_lotto": item.get("l645LtNtslYn") == "Y",
                    "sells_pension": item.get("pt720NtslYn") == "Y",
                    "sells_speetto_2000": item.get("st20LtNtslYn") == "Y",
                    "sells_speetto_1000": item.get("st10LtNtslYn") == "Y",
                    "sells_speetto_500": item.get("st5LtNtslYn") == "Y",
                })

            page += 1
            await _delay()

        except Exception as e:
            logger.error(f"[판매점] {sido} {sigungu} page={page} 실패: {e}")
            break

    logger.info(f"[판매점] {sido} {sigungu}: {len(stores)}건")
    return stores


# 로또 당첨 조회
async def crawl_lotto_round(round_no: int, client: httpx.AsyncClient | None = None) -> list[dict]:
    c = client or await _get_client()

    try:
        resp = await c.get(f"{BASE_URL}/lt645/selectPstLt645InfoNew.do", params={
            "srchDir": "center",
            "srchLtEpsd": round_no,
        })
        items = resp.json().get("data", {}).get("list", []) or []
    except Exception as e:
        logger.error(f"[로또] srchLtEpsd={round_no} 실패: {e}")
        return []

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
            logger.warning(f"[로또] round={rn} 파싱 실패: {e}")

    logger.info(f"[로또] srchLtEpsd={round_no}: {len(results)}회차 파싱")
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
        await conn.executemany(query, rows)

    logger.info(f"[DB] 로또 {len(rows)}건 저장 시도 (중복 제외)")
    return len(rows)


async def crawl_and_save_all_lotto_results(latest_round: int, start_round: int = 1) -> int:
    client = await _get_client()
    total = 0

    try:
        # 윈도우 크기가 6이므로 6회차 간격 + 마지막 latest_round 한 번 더
        calls = list(range(start_round + 5, latest_round + 1, 6))
        if not calls or calls[-1] != latest_round:
            calls.append(latest_round)

        for n in calls:
            results = await crawl_lotto_round(n, client=client)
            if results:
                saved = await save_lotto_results_to_db(results)
                total += saved
            await _delay()
    finally:
        await client.aclose()

    logger.info(f"[로또 크롤링 완료] 시도 건수 {total}")
    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    LATEST_ROUND = 1220

    async def _main():
        from app.core.database import close_pool
        try:
            await crawl_and_save_all_lotto_results(LATEST_ROUND)
        except Exception as e:
            logger.exception(f"실행 중 에러 발생: {e}")
        finally:
            await close_pool()

    asyncio.run(_main())

