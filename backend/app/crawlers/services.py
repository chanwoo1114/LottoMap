import asyncio
import logging
import re
from datetime import datetime
from typing import Optional

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
    await asyncio.sleep(settings.CRAWL_REQUEST_DELAY)


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


async def save_stores_to_db(stores: list[dict]) -> int:
    if not stores:
        return 0

    pool = await get_pool()
    query = """
        INSERT INTO stores (
            store_id, name, phone, address, address_detail,
            sido, sigungu, dong, location,
            sells_lotto, sells_pension,
            sells_speetto_2000, sells_speetto_1000, sells_speetto_500
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, ST_SetSRID(ST_MakePoint($9, $10), 4326),
            $11, $12, $13, $14, $15
        )
        ON CONFLICT (store_id) DO UPDATE SET
            name               = EXCLUDED.name,
            phone              = EXCLUDED.phone,
            address            = EXCLUDED.address,
            address_detail     = EXCLUDED.address_detail,
            sido               = EXCLUDED.sido,
            sigungu            = EXCLUDED.sigungu,
            dong               = EXCLUDED.dong,
            location           = EXCLUDED.location,
            sells_lotto        = EXCLUDED.sells_lotto,
            sells_pension      = EXCLUDED.sells_pension,
            sells_speetto_2000 = EXCLUDED.sells_speetto_2000,
            sells_speetto_1000 = EXCLUDED.sells_speetto_1000,
            sells_speetto_500  = EXCLUDED.sells_speetto_500,
            is_active          = TRUE,
            updated_at         = NOW()
    """

    rows = [
        (
            s["store_id"], s["name"], s["phone"], s["address"], s["address_detail"],
            s["sido"], s["sigungu"], s["dong"],
            float(s["lon"]) if s["lon"] else 0.0,
            float(s["lat"]) if s["lat"] else 0.0,
            s["sells_lotto"], s["sells_pension"],
            s["sells_speetto_2000"], s["sells_speetto_1000"], s["sells_speetto_500"],
        )
        for s in stores
    ]

    async with pool.acquire() as conn:
        await conn.executemany(query, rows)

    logger.info(f"[DB] {len(rows)}건 저장 완료")
    return len(rows)


async def crawl_and_save_all_stores():
    client = await _get_client()
    total = 0

    try:
        for sido, sigungu in ALL_REGION_PAIRS:
            stores = await crawl_store_location_by_region(sido, sigungu, client=client)
            if stores:
                saved = await save_stores_to_db(stores)
                total += saved
    finally:
        await client.aclose()

    logger.info(f"[크롤링 완료] 전체 {total}건 저장")
    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    async def _main():
        from app.core.database import close_pool

        try:
            total = await crawl_and_save_all_stores()
            print(f"\n===== 전체 {total}건 저장 완료 =====")
        except Exception as e:
            logger.exception(f"실행 중 에러 발생: {e}")
        finally:
            await close_pool()

    asyncio.run(_main())