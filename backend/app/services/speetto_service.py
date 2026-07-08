import asyncpg

_ONSALE_SELECT = """
    SELECT
        game_id, name, game_type, round_no, price,
        sale_end_date, prize_claim_end_date, image_url,
        total_first_prizes, remaining_first_prizes,
        total_second_prizes, remaining_second_prizes,
        total_third_prizes, remaining_third_prizes,
        intake_rate, updated_at
    FROM speetto_games
    WHERE sale_end_date >= CURRENT_DATE
    ORDER BY price DESC, round_no DESC
"""


async def get_onsale_games(pool: asyncpg.Pool) -> list[dict]:
    """판매기한이 지나지 않은 게임만. 2000 → 1000 → 500, 최신 회차 우선."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(_ONSALE_SELECT)
    return [dict(r) for r in rows]
