import asyncpg

_SELECT = """
    SELECT s.id AS store_id, s.name, s.address, s.sido, s.sigungu,
           ST_Y(s.location) AS lat, ST_X(s.location) AS lng,
           w.prize_rank, w.prize_amount, w.purchase_method
    FROM winning_stores w
    JOIN stores s ON s.id = w.store_id
"""

async def get_winning_stores(
    pool: asyncpg.Pool, lottery_type: str, round_no: int, prize_rank: int
) -> list[dict]:
    """회차·등수별 당첨 배출점 목록"""
    rows = await pool.fetch(
        f"""{_SELECT}
            WHERE w.lottery_type = $1 AND w.round_no = $2 AND w.prize_rank = $3
            ORDER BY s.name""",
        lottery_type, round_no, prize_rank,
    )
    return [dict(r) for r in rows]