import logging
import re

import httpx

from app.core.database import get_pool
from app.crawlers.common import BASE_URL, get_client, log_crawl_finish, log_crawl_start

logger = logging.getLogger(__name__)

_API_URL = f"{BASE_URL}/st/selectPblcnDsctn.do"
_API_HEADERS = {
    "AJAX": "true",
    "Referer": f"{BASE_URL}/st/pblcnDsctn",
}

TYPE_CD_MAP = {"SP2000": "st2000", "SP1000": "st1000", "SP500": "st500"}
RT_RE = re.compile(r"(\d[\d,]*)\s*매\s*/\s*(\d[\d,]*)\s*매")


UPSERT_SPEETTO_SQL = """
INSERT INTO speetto_games (
    game_id, name, game_type, round_no, price,
    is_on_sale,
    total_first_prizes, remaining_first_prizes,
    total_second_prizes, remaining_second_prizes,
    intake_rate, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10, $11, NOW()
)
ON CONFLICT (game_id) DO UPDATE SET
    name                    = EXCLUDED.name,
    game_type               = EXCLUDED.game_type,
    round_no                = EXCLUDED.round_no,
    price                   = EXCLUDED.price,
    is_on_sale              = EXCLUDED.is_on_sale,
    total_first_prizes      = EXCLUDED.total_first_prizes,
    remaining_first_prizes  = EXCLUDED.remaining_first_prizes,
    total_second_prizes     = EXCLUDED.total_second_prizes,
    remaining_second_prizes = EXCLUDED.remaining_second_prizes,
    intake_rate             = EXCLUDED.intake_rate,
    updated_at              = NOW()
"""


def _parse_rt(text: str | None) -> tuple[int, int]:
    '''"4매/6매" → (remaining=4, total=6). 비정상 값이면 (0, 0)'''
    if not text:
        return 0, 0
    m = RT_RE.search(text)
    if not m:
        return 0, 0
    remain = int(m.group(1).replace(",", ""))
    total = int(m.group(2).replace(",", ""))
    return remain, total


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
        "is_on_sale": item.get("ntslStatus") == "판매중",
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
    """판매중인 스피또 회차만 필터링해 반환"""
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

    rows: list[dict] = []
    for it in items:
        if it.get("ntslStatus") != "판매중":
            continue
        parsed = _parse_item(it)
        if parsed:
            rows.append(parsed)

    logger.info(f"[SPEETTO] 판매중 {len(rows)}회차 파싱 (total={data.get('total')})")
    return rows


async def save_speetto_to_db(games: list[dict]) -> int:
    if not games:
        return 0

    pool = await get_pool()
    rows = [
        (
            g["game_id"], g["name"], g["game_type"], g["round_no"], g["price"],
            g["is_on_sale"],
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


async def mark_sold_out_speetto(seen_ids: set[str]) -> int:
    """이번 크롤에서 안 보인 is_on_sale 회차를 매진 처리"""
    if not seen_ids:
        return 0
    pool = await get_pool()
    result = await pool.execute(
        """
        UPDATE speetto_games
        SET is_on_sale = FALSE, updated_at = NOW()
        WHERE is_on_sale = TRUE AND game_id <> ALL($1::varchar[])
        """,
        list(seen_ids),
    )
    return int(result.split()[-1])


async def crawl_and_save_speetto() -> dict:
    """판매중 스피또 현황을 크롤링·upsert, 매진 회차 처리"""
    log_id = await log_crawl_start("crawl_speetto")
    logger.info("[START] crawl_speetto")

    try:
        client = await get_client()
        try:
            games = await crawl_speetto_onsale(client)
        finally:
            await client.aclose()

        upserted = await save_speetto_to_db(games)
        seen_ids = {g["game_id"] for g in games}
        sold_out = await mark_sold_out_speetto(seen_ids)

        msg = f"upserted={upserted}, sold_out={sold_out}"
        await log_crawl_finish(log_id, "success", msg)
        logger.info(f"[END] crawl_speetto: {msg}")

        return {"upserted": upserted, "sold_out": sold_out, "games": games}
    except Exception as e:
        await log_crawl_finish(log_id, "failed", str(e))
        logger.exception(f"[FAIL] crawl_speetto: {e}")
        raise

