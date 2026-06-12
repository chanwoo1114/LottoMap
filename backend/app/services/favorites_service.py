import asyncpg
from fastapi import HTTPException, status

_STORE_SELECT = """
    SELECT s.id, s.store_id, s.name, s.address, s.phone, s.sido, s.sigungu, s.dong,
           s.sells_lotto, s.sells_pension,
           s.sells_speetto_2000, s.sells_speetto_1000, s.sells_speetto_500,
           ST_Y(s.location) AS lat, ST_X(s.location) AS lng
    FROM user_favorite_stores f
    JOIN stores s ON s.id = f.store_id
"""


async def get_my_favorites_list(pool: asyncpg.Pool, user_id: int) -> list[dict]:
    """내 즐겨찾기 판매점 목록"""
    rows = await pool.fetch(
        f"""{_STORE_SELECT}
            WHERE f.user_id = $1 AND s.is_active = TRUE
            ORDER BY f.created_at DESC""",
        user_id,
    )
    return [dict(r) for r in rows]

async def add_favorite(pool: asyncpg.Pool, user_id: int, store_id: int) -> None:
    """즐겨찾기 추가"""
    exists = await pool.fetchval("SELECT 1 FROM stores WHERE id = $1", store_id)

    if not exists:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "판매점을 찾을 수 없습니다.")
    await pool.execute(
        """INSERT INTO user_favorite_stores (user_id, store_id) VALUES ($1, $2)
           ON CONFLICT (user_id, store_id) DO NOTHING""",
        user_id, store_id,
    )

async def remove_favorite(pool: asyncpg.Pool, user_id: int, store_id: int) -> None:
    """즐겨찾기 제거"""
    await pool.execute(
        "DELETE FROM user_favorite_stores WHERE user_id = $1 AND store_id = $2",
        user_id, store_id,
    )