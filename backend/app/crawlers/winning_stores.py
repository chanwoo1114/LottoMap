import logging

import httpx

from app.core.database import get_pool
from app.crawlers.common import (
    BASE_URL, delay, get_client, insert_bootstrap_failure,
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

# 스피또 세부 종류 → API 게임 코드
_ST_GDS_CODE = {"st2000": "LP35", "st1000": "LP34", "st500": "LP33"}

# selectPblcnDsctn.do 의 stGmTypeCd → 내부 game_type
_SP_TYPE_CD = {"SP2000": "st2000", "SP1000": "st1000", "SP500": "st500"}

# 내부 game_type → winning_stores.lottery_type
_LOTTERY_TYPE = {
    "lt645": "lotto",
    "pt720": "pension",
    "st2000": "speetto_2000",
    "st1000": "speetto_1000",
    "st500": "speetto_500",
}

# 게임 타입 → 수집할 등수 (API srchWnShpRnk 값, 21=연금 보너스)
_RANK_CONFIG = {
    "lt645": [1, 2],
    "pt720": [1, 2, 21],
    "st2000": [1, 2],
    "st1000": [1],
    "st500": [1],
}

# atmtPsvYn → purchase_method
_ATMT_MAP = {"M": "manual", "B": "semi_auto", "Q": "auto"}


UPSERT_WINNING_SQL = """
INSERT INTO winning_stores (
    lottery_type, round_no, prize_rank,
    store_id, store_name, store_address, purchase_method
) VALUES (
    $1, $2, $3,
    (SELECT id FROM stores WHERE store_id = $4),
    $5, $6, $7
)
ON CONFLICT (lottery_type, round_no, prize_rank, store_name) DO NOTHING
"""


async def _fetch_winning(
    client: httpx.AsyncClient, game_type: str, round_no: int, rank: int
) -> list[dict]:
    if game_type == "lt645":
        url = _LT_URL
        params = {"srchWnShpRnk": rank, "srchLtEpsd": round_no, "srchShpLctn": ""}
    elif game_type == "pt720":
        url = _PT_URL
        params = {"srchWnShpRnk": rank, "srchLtEpsd": round_no, "srchShpLctn": ""}
    else:
        url = _ST_URL
        params = {
            "srchWnShpRnk": rank,
            "srchLtEpsd": round_no,
            "srchShpLctn": "",
            "srchLtGdsCd": _ST_GDS_CODE[game_type],
        }
    resp = await client.get(url, params=params, headers=_HEADERS)
    resp.raise_for_status()
    data = resp.json().get("data") or {}
    return data.get("list") or []


def _to_row(game_type: str, round_no: int, rank: int, item: dict) -> tuple | None:
    store_name = (item.get("shpNm") or "").strip()
    if not store_name:
        return None
    return (
        _LOTTERY_TYPE[game_type],
        round_no,
        rank,
        item.get("ltShpId"),
        store_name,
        (item.get("shpAddr") or "").strip(),
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


async def _get_rounds_from_db(table: str) -> list[int]:
    pool = await get_pool()
    rows = await pool.fetch(f"SELECT round_no FROM {table} ORDER BY round_no")
    return [r["round_no"] for r in rows]


async def _get_speetto_rounds(client: httpx.AsyncClient) -> dict[str, list[int]]:
    """selectPblcnDsctn.do 로 전체 스피또 회차 목록(판매중+판매종료)을 종류별로 수집"""
    resp = await client.get(
        _SPEETTO_LIST_URL,
        params={
            "pageNum": 1,
            "recordCountPerPage": 500,
            "gdsType": "",
            "gdsPrice": "",
            "gdsStatus": "",
        },
        headers=_SPEETTO_HEADERS,
    )
    resp.raise_for_status()
    items = resp.json().get("data", {}).get("list") or []

    result: dict[str, list[int]] = {"st2000": [], "st1000": [], "st500": []}
    for it in items:
        gt = _SP_TYPE_CD.get(it.get("stGmTypeCd"))
        rnd = it.get("stEpsd")
        if gt and rnd:
            result[gt].append(int(rnd))
    for gt in result:
        result[gt] = sorted(set(result[gt]))
    return result


async def crawl_all_winning_stores(
    delay_lo: int = 1, delay_hi: int = 3
) -> dict:
    """로또·연금·스피또 전체 회차의 당첨판매점 upsert.
    {"saved": N, "failures": [sub_keys]} 반환. sub_key는 'game_type/round/rank' 포맷."""
    logger.info("[START] crawl_winning_stores")

    total_saved = 0
    failures: list[str] = []
    lotto_rounds = await _get_rounds_from_db("lotto_results")
    pension_rounds = await _get_rounds_from_db("pension_results")

    client = await get_client()
    try:
        speetto_rounds = await _get_speetto_rounds(client)
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
            ranks = _RANK_CONFIG[game_type]
            logger.info(
                f"[WIN] {game_type}: rounds={len(rounds)}, ranks={ranks}"
            )
            for rnd in rounds:
                for rank in ranks:
                    sub_key = f"{game_type}/{rnd}/{rank}"
                    try:
                        items = await _fetch_winning(client, game_type, rnd, rank)
                        rows = [
                            r for it in items
                            if (r := _to_row(game_type, rnd, rank, it)) is not None
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
    sub_keys: list[str], delay_lo: int = 1, delay_hi: int = 3
) -> dict:
    """주어진 'game_type/round/rank' 리스트 재시도.
    {"resolved": [...], "still_failed": [...]} 반환."""
    if not sub_keys:
        return {"resolved": [], "still_failed": []}

    logger.info(f"[RETRY] winning {len(sub_keys)}건")
    resolved: list[str] = []
    still_failed: list[str] = []

    client = await get_client()
    try:
        for sub_key in sub_keys:
            try:
                game_type, rnd_s, rank_s = sub_key.split("/")
                rnd = int(rnd_s)
                rank = int(rank_s)
            except ValueError:
                logger.warning(f"[RETRY] winning sub_key 파싱 실패: {sub_key}")
                still_failed.append(sub_key)
                continue
            if game_type not in _RANK_CONFIG:
                logger.warning(f"[RETRY] winning 미지원 game_type: {game_type}")
                still_failed.append(sub_key)
                continue
            try:
                items = await _fetch_winning(client, game_type, rnd, rank)
                rows = [
                    r for it in items
                    if (r := _to_row(game_type, rnd, rank, it)) is not None
                ]
                if rows:
                    await _save_rows(rows)
                resolved.append(sub_key)
            except Exception as e:
                still_failed.append(sub_key)
                logger.warning(f"[RETRY] winning {sub_key} 여전히 실패: {e}")
            await delay(delay_lo, delay_hi)
    finally:
        await client.aclose()

    logger.info(
        f"[RETRY] winning: resolved={len(resolved)}, still_failed={len(still_failed)}"
    )
    return {"resolved": resolved, "still_failed": still_failed}