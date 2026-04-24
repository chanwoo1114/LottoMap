import asyncpg

from app.schema.store_schema import StoreQuery


async def search_stores(pool: asyncpg.Pool, q: StoreQuery) -> list[dict]:
    """판매점 검색 (지역/복권종류/주소)"""
    conditions = ["is_active = TRUE"]
    params: list = []
    idx = 1

    if q.sido:
        conditions.append(f"sido = ${idx}")
        params.append(q.sido)
        idx += 1

    if q.sigungu:
        conditions.append(f"sigungu = ${idx}")
        params.append(q.sigungu)
        idx += 1

    if q.address:
        conditions.append(f"address ILIKE '%' || ${idx} || '%'")
        params.append(q.address)
        idx += 1

    if q.sells_lotto:
        conditions.append("sells_lotto = TRUE")
    if q.sells_pension:
        conditions.append("sells_pension = TRUE")
    if q.sells_speetto_2000:
        conditions.append("sells_speetto_2000 = TRUE")
    if q.sells_speetto_1000:
        conditions.append("sells_speetto_1000 = TRUE")
    if q.sells_speetto_500:
        conditions.append("sells_speetto_500 = TRUE")

    where = " AND ".join(conditions)
    params.extend([q.size, (q.page - 1) * q.size])

    rows = await pool.fetch(f"""
        SELECT id, store_id, name, address, phone, sido, sigungu, dong,
               sells_lotto, sells_pension,
               sells_speetto_2000, sells_speetto_1000, sells_speetto_500,
               ST_Y(location) AS lat, ST_X(location) AS lng
        FROM stores
        WHERE {where}
        ORDER BY name
        LIMIT ${idx} OFFSET ${idx + 1}
    """, *params)

    return [dict(r) for r in rows]


async def get_store_by_id(pool: asyncpg.Pool, store_id: int) -> dict | None:
    """판매점 상세 조회"""
    row = await pool.fetchrow("""
        SELECT id, store_id, name, address, phone, sido, sigungu, dong,
               sells_lotto, sells_pension,
               sells_speetto_2000, sells_speetto_1000, sells_speetto_500,
               ST_Y(location) AS lat, ST_X(location) AS lng
        FROM stores
        WHERE id = $1 AND is_active = TRUE
    """, store_id)

    return dict(row) if row else None


async def get_nearby_stores(
    pool: asyncpg.Pool, lat: float, lng: float, radius_m: int
) -> list[dict]:
    """반경 내 판매점 조회 (가까운 순)"""
    rows = await pool.fetch("""
        SELECT id, store_id, name, address, phone,
               sells_lotto, sells_pension,
               sells_speetto_2000, sells_speetto_1000, sells_speetto_500,
               ST_Y(location) AS lat, ST_X(location) AS lng,
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
    """, lat, lng, radius_m)

    return [dict(r) for r in rows]