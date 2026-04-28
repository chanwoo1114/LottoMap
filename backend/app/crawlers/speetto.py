import logging
import re
from datetime import date

import httpx

from app.core.database import get_pool
from app.crawlers.common import (
    BASE_URL, get_client,
    insert_bootstrap_failure, resolve_bootstrap_failure,
)

logger = logging.getLogger(__name__)

_TASK_NAME = "crawl_speetto"

_API_URL = f"{BASE_URL}/st/selectPblcnDsctn.do"
_API_HEADERS = {
    "AJAX": "true",
    "Referer": f"{BASE_URL}/st/pblcnDsctn",
}
_IMG_BASE = f"{BASE_URL}/winImages"

TYPE_CD_MAP = {"SP2000": "st2000", "SP1000": "st1000", "SP500": "st500"}
RT_RE = re.compile(r"(\d[\d,]*)\s*매\s*/\s*(\d[\d,]*)\s*매")


UPSERT_SPEETTO_SQL = """
INSERT INTO speetto_games (
    game_id, name, game_type, round_no, price,
    sale_end_date, prize_claim_end_date, image_url,
    total_first_prizes, remaining_first_prizes,
    total_second_prizes, remaining_second_prizes,
    intake_rate, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8,
    $9, $10, $11, $12, $13, NOW()
)
ON CONFLICT (game_id) DO UPDATE SET
    name                    = EXCLUDED.name,
    game_type               = EXCLUDED.game_type,
    round_no                = EXCLUDED.round_no,
    price                   = EXCLUDED.price,
    sale_end_date           = EXCLUDED.sale_end_date,
    prize_claim_end_date    = EXCLUDED.prize_claim_end_date,
    image_url               = EXCLUDED.image_url,
    total_first_prizes      = EXCLUDED.total_first_prizes,
    remaining_first_prizes  = EXCLUDED.remaining_first_prizes,
    total_second_prizes     = EXCLUDED.total_second_prizes,
    remaining_second_prizes = EXCLUDED.remaining_second_prizes,
    intake_rate             = EXCLUDED.intake_rate,
    updated_at              = NOW()
"""


def _parse_rt(text: str | None) -> tuple[int, int]:
    """남아있는 매수 구분"""
    if not text:
        return 0, 0
    m = RT_RE.search(text)
    if not m:
        return 0, 0
    remain = int(m.group(1).replace(",", ""))
    total = int(m.group(2).replace(",", ""))
    return remain, total


def _parse_date(text: str | None) -> date | None:
    """'날짜 데이터 파싱"""
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _build_image_url(path: str | None) -> str | None:
    """API가 준 상대경로(/img/board/...)를 풀 URL(https://.../winImages/img/...)로 변환"""
    if not path:
        return None
    return f"{_IMG_BASE}{path}"


def _parse_item(item: dict) -> dict | None:
    type_cd = item.get("stGmTypeCd")
    game_type = TYPE_CD_MAP.get(type_cd)
    round_no = item.get("stEpsd")
    if not game_type or not round_no:
        return None

    remain1, total1 = _parse_rt(item.get("stRnk1Rt"))
    remain2, total2 = _parse_rt(item.get("stRnk2Rt"))

    return {
        "game_id": f"{game_type}_{round_no}",
        "name": item.get("stGmTypeNm") or game_type,
        "game_type": game_type,
        "round_no": int(round_no),
        "price": int(item.get("stNtslAmt") or 0),
        "sale_end_date": _parse_date(item.get("stNtslEndDt")),
        "prize_claim_end_date": _parse_date(item.get("stGiveEndDt")),
        "image_url": _build_image_url(item.get("stMainImgStrgPathNm")),
        "total_first_prizes": total1,
        "remaining_first_prizes": remain1,
        "total_second_prizes": total2,
        "remaining_second_prizes": remain2,
        "intake_rate": int(item.get("stSpmtRt") or 0),
    }


async def crawl_speetto_onsale(
    client: httpx.AsyncClient | None = None,
    page_size: int = 100,
) -> list[dict]:
    """판매기한이 오늘 이후인 스피또 회차만 반환."""
    c = client or await get_client()

    resp = await c.get(
        _API_URL,
        params={
            "pageNum": 1,
            "recordCountPerPage": page_size,
            "gdsType": "",
            "gdsPrice": "",
            "gdsStatus": "",
        },
        headers=_API_HEADERS,
    )
    resp.raise_for_status()
    data = resp.json().get("data") or {}
    items = data.get("list") or []

    today = date.today()
    rows: list[dict] = []
    for it in items:

        parsed = _parse_item(it)
        if not parsed:
            continue
        end = parsed["sale_end_date"]
        if end is None or end < today:
            continue
        rows.append(parsed)

    logger.info(
        f"[SPEETTO] 판매기한 내 {len(rows)}회차 파싱 (total={data.get('total')})"
    )
    return rows


async def save_speetto_to_db(games: list[dict]) -> int:
    if not games:
        return 0

    pool = await get_pool()
    rows = [
        (
            g["game_id"], g["name"], g["game_type"], g["round_no"], g["price"],
            g["sale_end_date"], g["prize_claim_end_date"], g["image_url"],
            g["total_first_prizes"], g["remaining_first_prizes"],
            g["total_second_prizes"], g["remaining_second_prizes"],
            g["intake_rate"],
        )
        for g in games
    ]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(UPSERT_SPEETTO_SQL, rows)

    logger.info(f"[DB] 스피또 {len(rows)}회차 upsert")
    return len(rows)


async def crawl_and_save_speetto() -> dict:
    """판매기한 내 스피또 현황 upsert. {"saved": N, "failures": [sub_keys]} 반환.
    sub_key는 'all' 단일."""
    logger.info("[START] crawl_speetto")

    try:
        client = await get_client()
        try:
            games = await crawl_speetto_onsale(client)
        finally:
            await client.aclose()

        upserted = await save_speetto_to_db(games)
        logger.info(f"[END] crawl_speetto: upserted={upserted}")
        return {"saved": upserted, "failures": []}
    except Exception as e:
        logger.exception(f"[FAIL] crawl_speetto: {e}")
        try:
            await insert_bootstrap_failure(_TASK_NAME, "all")
        except Exception as db_e:
            logger.warning(f"[FAIL-LOG] DB 기록 실패: {db_e}")
        return {"saved": 0, "failures": ["all"]}


async def retry_speetto_sub_keys(sub_keys: list[str]) -> dict:
    """speetto는 단일 API라 sub_key='all' 지원."""
    if not sub_keys:
        return {"resolved": [], "still_failed": []}

    logger.info(f"[RETRY] speetto {sub_keys}")
    resolved: list[str] = []
    still_failed: list[str] = []

    for sub_key in sub_keys:
        if sub_key != "all":
            logger.warning(f"[RETRY] speetto 미지원 sub_key: {sub_key}")
            await insert_bootstrap_failure(_TASK_NAME, sub_key)
            still_failed.append(sub_key)
            continue
        try:
            result = await crawl_and_save_speetto()
            if not result["failures"]:
                await resolve_bootstrap_failure(_TASK_NAME, sub_key)
                resolved.append(sub_key)
            else:
                await insert_bootstrap_failure(_TASK_NAME, sub_key)
                still_failed.append(sub_key)
        except Exception as e:
            await insert_bootstrap_failure(_TASK_NAME, sub_key)
            still_failed.append(sub_key)
            logger.warning(f"[RETRY] speetto 여전히 실패: {e}")

    return {"resolved": resolved, "still_failed": still_failed}