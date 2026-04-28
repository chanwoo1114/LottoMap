import logging

import httpx

from app.core.database import get_pool
from app.crawlers.common import (
    BASE_URL, delay, get_client,
    insert_bootstrap_failure, resolve_bootstrap_failure,
)

logger = logging.getLogger(__name__)

_TASK_NAME = "crawl_winning"

_LT_URL = f"{BASE_URL}/wnprchsplcsrch/selectLtWnShp.do"
_PT_URL = f"{BASE_URL}/wnprchsplcsrch/selectPtWnShp.do"
_ST_URL = f"{BASE_URL}/wnprchsplcsrch/selectStWnShp.do"
_SPEETTO_LIST_URL = f"{BASE_URL}/st/selectPblcnDsctn.do"

_HEADERS = {
    "AJAX": "true",
    "Referer": f"{BASE_URL}/wnprchsplcsrch/home",
}
_SPEETTO_HEADERS = {
    "AJAX": "true",
    "Referer": f"{BASE_URL}/st/pblcnDsctn",
}

_ST_GDS_CODE = {"st2000": "LP35", "st1000": "LP34", "st500": "LP33"}

_SP_TYPE_CD = {"SP2000": "st2000", "SP1000": "st1000", "SP500": "st500"}

_LOTTERY_TYPE = {
    "lt645": "lotto",
    "pt720": "pension",
    "st2000": "speetto_2000",
    "st1000": "speetto_1000",
    "st500": "speetto_500",
}

_RANK_FILTER: dict[str, set[int]] = {
    "lt645": {1, 2},
    "pt720": {1, 2, 21},
    "st2000": {1, 2},
    "st1000": {1},
    "st500": {1},
}

_ST_MIN_ROUND: dict[str, int] = {
    "st2000": 14,
    "st1000": 16,
    "st500": 18,
}

_ATMT_MAP = {"M": "manual", "B": "semi_auto", "Q": "auto"}


UPSERT_WINNING_SQL = """
WITH new_store AS (
    INSERT INTO stores (
        store_id, name, address, phone,
        sido, sigungu, dong,
        location,
        sells_lotto, sells_pension,
        sells_speetto_2000, sells_speetto_1000, sells_speetto_500,
        is_active
    )
    VALUES (
        $4, $5, $6, $7,
        $8, $9, $10,
        ST_SetSRID(ST_MakePoint($11, $12), 4326),
        $13, $14, $15, $16, $17,
        FALSE
    )
    ON CONFLICT (store_id) DO NOTHING
    RETURNING id
),
sid AS (
    (SELECT id FROM new_store)
    UNION ALL
    (SELECT id FROM stores WHERE store_id = $4)
    LIMIT 1
)
INSERT INTO winning_stores (
    lottery_type, round_no, prize_rank,
    store_id, purchase_method
)
SELECT $1, $2, $3, sid.id, $18 FROM sid
ON CONFLICT (lottery_type, round_no, prize_rank, store_id) DO NOTHING
"""


async def _fetch_winning(
    client: httpx.AsyncClient, game_type: str, round_no: int
) -> list[dict]:
    if game_type == "lt645":
        url = _LT_URL
        params = {"srchWnShpRnk": "all", "srchLtEpsd": round_no, "srchShpLctn": ""}
    elif game_type == "pt720":
        url = _PT_URL
        params = {"srchWnShpRnk": "all", "srchLtEpsd": round_no, "srchShpLctn": ""}
    else:
        url = _ST_URL
        params = {
            "srchWnShpRnk": "all",
            "srchLtEpsd": round_no,
            "srchShpLctn": "",
            "srchLtGdsCd": _ST_GDS_CODE[game_type],
        }
    resp = await client.get(url, params=params, headers=_HEADERS)
    resp.raise_for_status()
    data = resp.json().get("data") or {}
    items = data.get("list") or []
    sample = items[0] if items else None
    logger.info(
        f"[FETCH] {game_type} round={round_no}: "
        f"items={len(items)} status={resp.status_code} elapsed={resp.elapsed.total_seconds():.2f}s "
        f"sample_keys={list(sample.keys()) if sample else None} "
        f"sample_wnShpRnk={sample.get('wnShpRnk') if sample else None}"
    )
    return items


def _yn(v) -> bool:
    return v == "Y"


def _to_row(game_type: str, round_no: int, item: dict) -> tuple | None:
    try:
        actual_rank = int(item.get("wnShpRnk") or 0)
    except (TypeError, ValueError):
        return None
    if actual_rank not in _RANK_FILTER[game_type]:
        return None

    store_name = (item.get("shpNm") or "").strip()
    if not store_name:
        return None
    raw_id = item.get("ltShpId")
    lt_shp_id = str(raw_id).strip() if raw_id is not None else ""
    if not lt_shp_id:
        return None

    lat_raw = item.get("shpLat")
    lon_raw = item.get("shpLot")
    try:
        lat = float(lat_raw) if lat_raw is not None else None
        lon = float(lon_raw) if lon_raw is not None else None
    except (TypeError, ValueError):
        lat = lon = None

    return (
        _LOTTERY_TYPE[game_type],
        round_no,
        actual_rank,
        lt_shp_id,
        store_name,
        (item.get("shpAddr") or "").strip(),
        (item.get("shpTelno") or "").strip(),
        (item.get("tm1ShpLctnAddr") or "").strip(),
        (item.get("tm2ShpLctnAddr") or "").strip(),
        (item.get("tm3ShpLctnAddr") or "").strip(),
        lon,
        lat,
        _yn(item.get("l645LtNtslYn")),
        _yn(item.get("pt720NtslYn")),
        _yn(item.get("st20LtNtslYn")),
        _yn(item.get("st10LtNtslYn")),
        _yn(item.get("st5LtNtslYn")),
        _ATMT_MAP.get(item.get("atmtPsvYn") or "", "unknown"),
    )


async def _save_rows(rows: list[tuple]) -> int:
    if not rows:
        return 0
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(UPSERT_WINNING_SQL, rows)
    return len(rows)


async def _get_rounds_from_db(table: str, min_round: int = 1) -> list[int]:
    pool = await get_pool()
    rows = await pool.fetch(
        f"SELECT round_no FROM {table} WHERE round_no >= $1 ORDER BY round_no",
        min_round,
    )
    return [r["round_no"] for r in rows]


async def _get_speetto_rounds() -> dict[str, list[int]]:
    """speetto_games 테이블에서 종류별 max round 만 조회 후
    _ST_MIN_ROUND ~ max 전 범위 생성."""
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT game_type, MAX(round_no) AS max_r FROM speetto_games GROUP BY game_type"
    )
    max_per_type: dict[str, int] = {r["game_type"]: r["max_r"] for r in rows}

    result: dict[str, list[int]] = {}
    for gt, min_r in _ST_MIN_ROUND.items():
        max_r = max_per_type.get(gt, 0)
        result[gt] = list(range(min_r, max_r + 1)) if max_r >= min_r else []
    return result


async def crawl_all_winning_stores(
    delay_lo: int = 5, delay_hi: int = 10
) -> dict:
    """로또·연금·스피또 전체 회차의 당첨판매점 upsert.
    {"saved": N, "failures": [sub_keys]} 반환. sub_key는 'game_type/round/rank' 포맷."""
    logger.info("[START] crawl_winning_stores")

    total_saved = 0
    failures: list[str] = []
    # selectLtWnShp.do 는 1~216 회차 데이터 미제공, 262 부터 안정적
    lotto_rounds = await _get_rounds_from_db("lotto_results", min_round=262)
    pension_rounds = await _get_rounds_from_db("pension_results")
    logger.info(
        f"[PLAN] lotto rounds from DB: {len(lotto_rounds)}건 "
        f"(범위: {lotto_rounds[0] if lotto_rounds else None}~{lotto_rounds[-1] if lotto_rounds else None})"
    )

    speetto_rounds = await _get_speetto_rounds()
    logger.info(
        f"[PLAN] speetto rounds: "
        f"st2000={len(speetto_rounds.get('st2000') or [])} "
        f"(범위: {min(speetto_rounds.get('st2000') or [0])}~{max(speetto_rounds.get('st2000') or [0])}), "
        f"st1000={len(speetto_rounds.get('st1000') or [])} "
        f"(범위: {min(speetto_rounds.get('st1000') or [0])}~{max(speetto_rounds.get('st1000') or [0])}), "
        f"st500={len(speetto_rounds.get('st500') or [])} "
        f"(범위: {min(speetto_rounds.get('st500') or [0])}~{max(speetto_rounds.get('st500') or [0])})"
    )

    client = await get_client()
    try:
        plan = [
            ("lt645", lotto_rounds),
            ("pt720", pension_rounds),
            ("st2000", speetto_rounds.get("st2000") or []),
            ("st1000", speetto_rounds.get("st1000") or []),
            ("st500", speetto_rounds.get("st500") or []),
        ]
        for game_type, rounds in plan:
            if not rounds:
                logger.warning(f"[WIN] {game_type} 회차 목록 없음, skip")
                continue
            logger.info(f"[WIN] {game_type}: rounds={len(rounds)}")
            for rnd in rounds:
                sub_key = f"{game_type}/{rnd}"
                try:
                    items = await _fetch_winning(client, game_type, rnd)
                    rows = [
                        r for it in items
                        if (r := _to_row(game_type, rnd, it)) is not None
                    ]
                    if rows:
                        total_saved += await _save_rows(rows)
                except Exception as e:
                    failures.append(sub_key)
                    try:
                        await insert_bootstrap_failure(_TASK_NAME, sub_key)
                    except Exception as db_e:
                        logger.warning(f"[FAIL-LOG] DB 기록 실패: {db_e}")
                    logger.warning(f"[WIN] {sub_key} 실패: {e}")
                await delay(delay_lo, delay_hi)
    finally:
        await client.aclose()

    logger.info(
        f"[END] crawl_winning_stores: saved={total_saved}, failures={len(failures)}"
    )
    return {"saved": total_saved, "failures": failures}


async def retry_winning_sub_keys(
    sub_keys: list[str], delay_lo: int = 5, delay_hi: int = 10
) -> dict:
    """주어진 'game_type/round' 리스트 재시도.
    {"resolved": [...], "still_failed": [...]} 반환."""
    if not sub_keys:
        return {"resolved": [], "still_failed": []}

    logger.info(f"[RETRY] winning {len(sub_keys)}건")
    resolved: list[str] = []
    still_failed: list[str] = []

    client = await get_client()
    try:
        for sub_key in sub_keys:
            parts = sub_key.split("/")
            if len(parts) != 2:
                logger.warning(f"[RETRY] winning sub_key 파싱 실패: {sub_key}")
                await insert_bootstrap_failure(_TASK_NAME, sub_key)
                still_failed.append(sub_key)
                continue
            game_type, rnd_s = parts
            try:
                rnd = int(rnd_s)
            except ValueError:
                logger.warning(f"[RETRY] winning round 파싱 실패: {sub_key}")
                await insert_bootstrap_failure(_TASK_NAME, sub_key)
                still_failed.append(sub_key)
                continue
            if game_type not in _RANK_FILTER:
                logger.warning(f"[RETRY] winning 미지원 game_type: {game_type}")
                await insert_bootstrap_failure(_TASK_NAME, sub_key)
                still_failed.append(sub_key)
                continue
            try:
                items = await _fetch_winning(client, game_type, rnd)
                rows = [
                    r for it in items
                    if (r := _to_row(game_type, rnd, it)) is not None
                ]
                if rows:
                    await _save_rows(rows)
                await resolve_bootstrap_failure(_TASK_NAME, sub_key)
                resolved.append(sub_key)
            except Exception as e:
                await insert_bootstrap_failure(_TASK_NAME, sub_key)
                still_failed.append(sub_key)
                logger.warning(f"[RETRY] winning {sub_key} 여전히 실패: {e}")
            await delay(delay_lo, delay_hi)
    finally:
        await client.aclose()

    logger.info(
        f"[RETRY] winning: resolved={len(resolved)}, still_failed={len(still_failed)}"
    )
    return {"resolved": resolved, "still_failed": still_failed}