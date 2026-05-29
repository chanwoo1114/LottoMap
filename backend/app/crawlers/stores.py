"""판매점 위치 크롤러 - 전국 페이지네이션 방식"""
import logging

import httpx

from app.core.database import get_pool
from app.crawlers.common import (
    BASE_URL, delay, get_client,
    insert_bootstrap_failure, resolve_bootstrap_failure,
)
from app.crawlers.regions import CTPV_MAP

logger = logging.getLogger(__name__)

_TASK_NAME = "crawl_stores"
# API가 recordCountPerPage 값을 무시하고 페이지당 항상 10건만 반환함
_PAGE_SIZE = 10
# 빈 응답이 이 횟수만큼 연속되면 데이터 끝으로 간주 (일시적 200+빈본문 흡수용)
_EMPTY_RETRY_LIMIT = 3
# 수집 커버리지(seen/total)가 이 비율 미만이면 폐업 처리를 건너뜀 (대량 오폐업 방지)
_COMPLETENESS_THRESHOLD = 0.95

# API 응답의 시도 약칭 → 정식 명칭 (예: "서울" → "서울특별시")
_INV_CTPV_MAP: dict[str, str] = {v: k for k, v in CTPV_MAP.items()}


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


def _parse_item(item: dict) -> dict | None:
    """API 응답 한 건을 DB 저장용 dict로 변환. 필수 필드 없으면 None."""
    store_id = item.get("ltShpId")
    if not store_id:
        return None

    sido_short = (item.get("tm1BplcLctnAddr") or "").strip()
    sido = _INV_CTPV_MAP.get(sido_short, sido_short)

    return {
        "store_id": str(store_id),
        "name": (item.get("conmNm") or "").strip(),
        "phone": item.get("shpTelno") or "",
        "address": (item.get("bplcRdnmDaddr") or "").strip(),
        "address_detail": item.get("bplcLctnDaddr") or "",
        "sido": sido,
        "sigungu": (item.get("tm2BplcLctnAddr") or "").strip(),
        "dong": (item.get("tm3BplcLctnAddr") or "").strip(),
        "lat": item.get("shpLat"),
        "lng": item.get("shpLot"),
        "sells_lotto": item.get("l645LtNtslYn") == "Y",
        "sells_pension": item.get("pt720NtslYn") == "Y",
        "sells_speetto_2000": item.get("st20LtNtslYn") == "Y",
        "sells_speetto_1000": item.get("st10LtNtslYn") == "Y",
        "sells_speetto_500": item.get("st5LtNtslYn") == "Y",
    }


async def _fetch_page(client: httpx.AsyncClient, page: int) -> dict:
    """한 페이지 받아옴. JSON의 data 부분 반환."""
    resp = await client.get(
        f"{BASE_URL}/prchsplcsrch/selectLtShp.do",
        params={
            "pageNum": page,
            "recordCountPerPage": _PAGE_SIZE,
            "pageCount": 5,
            "srchCtpvNm": "",
            "srchSggNm": "",
        },
    )
    resp.raise_for_status()
    return resp.json().get("data") or {}


async def upsert_stores(stores: list[dict]) -> int:
    """판매점 리스트 upsert, 처리 건수 반환"""
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
    """이번 크롤에서 못 본 active 판매점을 is_active=FALSE 처리"""
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
    """전체 판매점을 글로벌 페이지네이션으로 크롤.
    빈 리스트가 나올 때까지 페이지를 증가시키며 수집하고, 마지막에
    커버리지(seen/total)가 충분할 때만 폐업 처리를 수행한다.
    {"upserted": N, "closed": N, "failures": [sub_keys], "total": N, "seen": N}
    반환. 실패 sub_key는 'page:N' 포맷."""
    logger.info("[START] crawl_stores")

    seen: set[str] = set()
    failures: list[str] = []
    total_upserted = 0
    total = 0

    client = await get_client()
    try:
        # 1페이지를 먼저 받아 total(전국 총 건수) 확보
        first = await _fetch_page(client, 1)
        total = int(first.get("total") or 0)
        if total == 0:
            logger.warning("[STORES] total=0, 응답 비어있음 — 중단")
            return {"upserted": 0, "closed": 0, "failures": [],
                    "total": 0, "seen": 0}
        logger.info(f"[STORES] total={total} (기대 페이지 ≈ {(total + _PAGE_SIZE - 1) // _PAGE_SIZE})")

        page = 1
        empty_streak = 0
        # 무한 루프 방지: 페이지 수는 절대 total을 넘을 수 없음(페이지당 ≥1건)
        max_page = total + 1
        while page <= max_page:
            sub_key = f"page:{page}"
            if page > 1:
                await delay()

            # 1페이지는 위에서 이미 받았으므로 재사용
            if page == 1:
                data = first
            else:
                try:
                    data = await _fetch_page(client, page)
                except Exception as e:
                    failures.append(sub_key)
                    try:
                        await insert_bootstrap_failure(_TASK_NAME, sub_key)
                    except Exception as db_e:
                        logger.warning(f"[FAIL-LOG] {sub_key} DB 기록 실패: {db_e}")
                    logger.error(f"[FAIL] {sub_key}: {e} — 다음 페이지로")
                    page += 1
                    continue

            items = data.get("list") or []

            # 빈 응답: 진짜 끝인지 일시적 빈본문인지 구분 위해 같은 페이지 재시도
            if not items:
                empty_streak += 1
                logger.warning(
                    f"[STORES] {sub_key} 빈 응답 "
                    f"({empty_streak}/{_EMPTY_RETRY_LIMIT})"
                )
                if empty_streak >= _EMPTY_RETRY_LIMIT:
                    logger.info(
                        f"[STORES] 빈 응답 {_EMPTY_RETRY_LIMIT}회 연속 → "
                        f"데이터 끝으로 판단 (last page={page})"
                    )
                    break
                continue  # 같은 page 재시도
            empty_streak = 0

            stores = [s for it in items if (s := _parse_item(it)) is not None]
            skipped = len(items) - len(stores)
            if stores:
                total_upserted += await upsert_stores(stores)
                seen.update(s["store_id"] for s in stores)
            logger.info(
                f"[STORES] {sub_key}: 수신={len(items)}, 저장={len(stores)}"
                f"{f', skip={skipped}' if skipped else ''} | "
                f"누적 seen={len(seen)}/{total} ({len(seen) / total:.1%})"
            )
            page += 1
        else:
            logger.warning(f"[STORES] max_page({max_page}) 도달 — 비정상 종료")
    finally:
        await client.aclose()

    # 폐업 처리: 크롤이 충분히 완전했을 때만 (대량 오폐업 방지)
    closed_count = 0
    coverage = len(seen) / total if total else 0.0
    if failures:
        logger.warning(
            f"[STORES] 실패 {len(failures)}건 존재 → 폐업 처리 스킵 "
            f"(seen={len(seen)}/{total})"
        )
    elif not seen:
        logger.warning("[STORES] seen 비어있음 → 폐업 처리 스킵")
    elif coverage < _COMPLETENESS_THRESHOLD:
        logger.warning(
            f"[STORES] 커버리지 부족 ({len(seen)}/{total}={coverage:.1%} "
            f"< {_COMPLETENESS_THRESHOLD:.0%}) → 폐업 처리 스킵"
        )
    else:
        logger.info(
            f"[STORES] 커버리지 {coverage:.1%} → 폐업 처리 진행"
        )
        closed_count = await mark_closed_stores(seen)
        logger.info(f"[STORES] 폐업 처리 완료: {closed_count}건 비활성화")

    ok = not failures and total > 0 and coverage >= _COMPLETENESS_THRESHOLD
    logger.info(
        f"[END] crawl_stores: {'성공' if ok else '불완전'} | "
        f"upserted={total_upserted}, closed={closed_count}, "
        f"seen={len(seen)}/{total} ({coverage:.1%}), failures={len(failures)}"
    )
    return {
        "upserted": total_upserted,
        "closed": closed_count,
        "failures": failures,
        "total": total,
        "seen": len(seen),
    }


async def retry_stores_sub_keys(sub_keys: list[str]) -> dict:
    """'page:N' 리스트 재시도. {"resolved": [...], "still_failed": [...]} 반환."""
    if not sub_keys:
        return {"resolved": [], "still_failed": []}

    logger.info(f"[RETRY] stores {len(sub_keys)}건")
    resolved: list[str] = []
    still_failed: list[str] = []

    client = await get_client()
    try:
        for sub_key in sub_keys:
            if not sub_key.startswith("page:"):
                logger.warning(f"[RETRY] stores 미지원 sub_key (legacy?): {sub_key}")
                await insert_bootstrap_failure(_TASK_NAME, sub_key)
                still_failed.append(sub_key)
                continue
            try:
                page = int(sub_key.split(":", 1)[1])
            except ValueError:
                logger.warning(f"[RETRY] stores page 파싱 실패: {sub_key}")
                await insert_bootstrap_failure(_TASK_NAME, sub_key)
                still_failed.append(sub_key)
                continue
            try:
                data = await _fetch_page(client, page)
                items = data.get("list") or []
                stores = [s for it in items if (s := _parse_item(it)) is not None]
                if stores:
                    await upsert_stores(stores)
                await resolve_bootstrap_failure(_TASK_NAME, sub_key)
                resolved.append(sub_key)
            except Exception as e:
                await insert_bootstrap_failure(_TASK_NAME, sub_key)
                still_failed.append(sub_key)
                logger.warning(f"[RETRY] stores {sub_key} 여전히 실패: {e}")
            await delay()
    finally:
        await client.aclose()

    logger.info(
        f"[RETRY] stores: resolved={len(resolved)}, still_failed={len(still_failed)}"
    )
    return {"resolved": resolved, "still_failed": still_failed}
