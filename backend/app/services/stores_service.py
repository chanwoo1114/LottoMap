import asyncpg


async def get_store_by_id(pool: asyncpg.Pool, store_id: int) -> dict | None:
    """판매점 상세 조회"""
    row = await pool.fetchrow("""
        SELECT id, store_id, name, address, phone, sido, sigungu, dong,
               sells_lotto, sells_pension,
               sells_speetto_2000, sells_speetto_1000, sells_speetto_500,
               ST_Y(location) AS lat, ST_X(location) AS lon
        FROM stores
        WHERE id = $1 AND is_active = TRUE
    """, store_id)

    return dict(row) if row else None


async def get_nearby_stores(
    pool: asyncpg.Pool, latitude: float, longitude: float, radius_m: int
) -> list[dict]:
    """반경 내 판매점 조회 (가까운 순)"""
    rows = await pool.fetch("""
        SELECT id, store_id, name, address, phone,
               sells_lotto, sells_pension,
               sells_speetto_2000, sells_speetto_1000, sells_speetto_500,
               ST_Y(location) AS lat, ST_X(location) AS lon,
               ST_DistanceSphere(
                   location,
                   ST_SetSRID(ST_MakePoint($2, $1), 4326)
               )::int AS distance_m
        FROM stores
        WHERE is_active = TRUE
          AND ST_DWithin(
              location::geography,
              ST_SetSRID(ST_MakePoint($2, $1), 4326)::geography,
              $3
          )
        ORDER BY distance_m ASC
    """, latitude, longitude, radius_m)

    return [dict(r) for r in rows]