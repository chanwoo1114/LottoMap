"""판매점 위치 크롤러"""
import logging

import httpx

from app.core.database import get_pool
from app.crawlers.common import (
    BASE_URL, delay, get_client, insert_bootstrap_failure,
)
from app.crawlers.regions import ALL_REGION_PAIRS, CTPV_MAP

logger = logging.getLogger(__name__)

_TASK_NAME = "crawl_stores"


UPSERT_STORE_SQL = """
INSERT INTO stores (
    store_id, name, address, address_detail, phone,
    sido, sigungu, dong,
    sells_lotto, sells_pension,
    sells_speetto_2000, sells_speetto_1000, sells_speetto_500,
    location, is_active
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
    CASE
        WHEN $14::float8 IS NOT NULL AND $15::float8 IS NOT NULL
        THEN ST_SetSRID(ST_MakePoint($15, $14), 4326)
        ELSE NULL
    END,
    TRUE
)
ON CONFLICT (store_id) DO UPDATE SET
    name               = EXCLUDED.name,
    address            = EXCLUDED.address,
    address_detail     = EXCLUDED.address_detail,
    phone              = EXCLUDED.phone,
    sido               = EXCLUDED.sido,
    sigungu            = EXCLUDED.sigungu,
    dong               = EXCLUDED.dong,
    sells_lotto        = EXCLUDED.sells_lotto,
    sells_pension      = EXCLUDED.sells_pension,
    sells_speetto_2000 = EXCLUDED.sells_speetto_2000,
    sells_speetto_1000 = EXCLUDED.sells_speetto_1000,
    sells_speetto_500  = EXCLUDED.sells_speetto_500,
    location           = COALESCE(EXCLUDED.location, stores.location),
    is_active          = TRUE
"""


async def crawl_store_location_by_region(
    sido: str,
    sigungu: str,
    client: httpx.AsyncClient | None = None,
    max_pages: int = 200,
) -> list[dict]:
    """한 시군구의 모든 판매점을 크롤링해 dict 리스트로 반환"""
    c = client or await get_client()
    stores: list[dict] = []
    page = 1

    while page <= max_pages:
        resp = await c.get(f"{BASE_URL}/prchsplcsrch/selectLtShp.do", params={
            "srchCtpvNm": CTPV_MAP[sido],
            "srchSggNm": sigungu,
            "pageNum": page,
            "recordCountPerPage": 10,
            "pageCount": 5,
        })
        resp.raise_for_status()
        data = resp.json().get("data", {})
        if not data:
            break
        items = data.get("list", [])
        if not items:
            break

        for item in items:
            store_id = item.get("ltShpId") or ""
            if not store_id:
                logger.warning(
                    f"[STORES] {sido} {sigungu} store_id 없음, skip: {item.get('conmNm')}"
                )
                continue
            stores.append({
                "store_id": store_id,
                "name": item.get("conmNm", ""),
                "phone": item.get("shpTelno") or "",
                "address": item.get("bplcRdnmDaddr", "").strip(),
                "address_detail": item.get("bplcLctnDaddr") or "",
                "sido": sido,
                "sigungu": sigungu,
                "dong": (item.get("tm3BplcLctnAddr") or "").strip(),
                "lat": item.get("shpLat"),
                "lng": item.get("shpLot"),
                "sells_lotto": item.get("l645LtNtslYn") == "Y",
                "sells_pension": item.get("pt720NtslYn") == "Y",
                "sells_speetto_2000": item.get("st20LtNtslYn") == "Y",
                "sells_speetto_1000": item.get("st10LtNtslYn") == "Y",
                "sells_speetto_500": item.get("st5LtNtslYn") == "Y",
            })

        page += 1
        await delay()
    else:
        logger.warning(f"[STORES] {sido} {sigungu} max_pages({max_pages}) 도달")

    logger.info(f"[STORES] {sido} {sigungu}: {len(stores)}건")
    return stores


async def upsert_stores(stores: list[dict]) -> int:
    """판매점 리스트를 stores 테이블에 upsert (시군구 단위 트랜잭션), 처리 건수 반환"""
    if not stores:
        return 0
    pool = await get_pool()
    rows = [
        (
            s["store_id"], s["name"], s["address"], s["address_detail"], s["phone"],
            s["sido"], s["sigungu"], s["dong"],
            s["sells_lotto"], s["sells_pension"],
            s["sells_speetto_2000"], s["sells_speetto_1000"], s["sells_speetto_500"],
            s["lat"], s["lng"],
        )
        for s in stores
    ]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(UPSERT_STORE_SQL, rows)
    return len(rows)


async def mark_closed_stores(seen_store_ids: set[str]) -> int:
    """이번 크롤에서 못 본 active 판매점을 is_active=FALSE 처리, 처리 건수 반환"""
    if not seen_store_ids:
        return 0
    pool = await get_pool()
    result = await pool.execute(
        """
        UPDATE stores
        SET is_active = FALSE
        WHERE is_active = TRUE AND store_id <> ALL($1::varchar[])
        """,
        list(seen_store_ids),
    )

    return int(result.split()[-1])


async def crawl_all_stores() -> dict:
    """전체 시군구 순회. {"upserted": N, "closed": N, "failures": [sub_keys]} 반환.
    실패 sub_key는 'sido/sigungu' 포맷."""
    logger.info("[START] crawl_stores")

    seen: set[str] = set()
    failures: list[str] = []
    total_upserted = 0

    client = await get_client()
    try:
        for sido, sigungu in ALL_REGION_PAIRS:
            sub_key = f"{sido}/{sigungu}"
            try:
                stores = await crawl_store_location_by_region(sido, sigungu, client)
                await upsert_stores(stores)
                seen.update(s["store_id"] for s in stores)
                total_upserted += len(stores)
            except Exception as e:
                failures.append(sub_key)
                try:
                    await insert_bootstrap_failure(_TASK_NAME, sub_key)
                except Exception as db_e:
                    logger.warning(f"[FAIL-LOG] DB 기록 실패: {db_e}")
                logger.error(f"[FAIL] {sub_key}: {e}")
    finally:
        await client.aclose()

    # 실패가 있으면 불완전한 데이터로 판단, mark_closed 건너뜀 (다음 retry 후 수행)
    closed_count = 0
    if not failures and seen:
        closed_count = await mark_closed_stores(seen)

    logger.info(
        f"[END] crawl_stores: upserted={total_upserted}, closed={closed_count}, "
        f"failures={len(failures)}"
    )
    return {
        "upserted": total_upserted,
        "closed": closed_count,
        "failures": failures,
    }


async def retry_stores_sub_keys(sub_keys: list[str]) -> dict:
    """주어진 'sido/sigungu' 리스트 재시도. {"resolved": [...], "still_failed": [...]} 반환."""
    if not sub_keys:
        return {"resolved": [], "still_failed": []}

    logger.info(f"[RETRY] stores {len(sub_keys)}건")
    resolved: list[str] = []
    still_failed: list[str] = []

    client = await get_client()
    try:
        for sub_key in sub_keys:
            try:
                sido, sigungu = sub_key.split("/", 1)
            except ValueError:
                logger.warning(f"[RETRY] stores sub_key 파싱 실패: {sub_key}")
                still_failed.append(sub_key)
                continue
            try:
                stores = await crawl_store_location_by_region(sido, sigungu, client)
                await upsert_stores(stores)
                resolved.append(sub_key)
            except Exception as e:
                still_failed.append(sub_key)
                logger.warning(f"[RETRY] stores {sub_key} 여전히 실패: {e}")
    finally:
        await client.aclose()

    logger.info(
        f"[RETRY] stores: resolved={len(resolved)}, still_failed={len(still_failed)}"
    )
    return {"resolved": resolved, "still_failed": still_failed}