import asyncpg

from app.schema.store_schema import StoreQuery


async def search_stores(pool: asyncpg.Pool, q: StoreQuery) -> list[dict]:
    """판매점 검색 (지역/복권종류/주소)"""
    conditions = ["is_active = TRUE", "is_online = FALSE"]
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


async def get_winning_stats(pool: asyncpg.Pool, store_id: int) -> dict:
    """판매점의 복권 종류·등수별 당첨 배출 횟수 집계"""
    rows = await pool.fetch("""
        SELECT lottery_type, prize_rank, COUNT(*)::int AS cnt
        FROM winning_stores
        WHERE store_id = $1
        GROUP BY lottery_type, prize_rank
    """, store_id)

    stats = {
        "lotto_first": 0,
        "lotto_second": 0,
        "pension_first": 0,
        "pension_second": 0,
        "speetto_first": 0,
        "speetto_second": 0,
        "total": 0,
    }
    for r in rows:
        lt, rank, cnt = r["lottery_type"], r["prize_rank"], r["cnt"]
        stats["total"] += cnt
        if lt == "lotto":
            if rank == 1:
                stats["lotto_first"] += cnt
            elif rank == 2:
                stats["lotto_second"] += cnt
        elif lt == "pension":
            if rank == 1:
                stats["pension_first"] += cnt
            elif rank == 2:
                stats["pension_second"] += cnt
        elif lt.startswith("speetto"):
            if rank == 1:
                stats["speetto_first"] += cnt
            elif rank == 2:
                stats["speetto_second"] += cnt
    return stats


async def get_nearby_stores(
    pool: asyncpg.Pool,
    min_lat: float, min_lng: float,
    max_lat: float, max_lng: float,
    limit: int
) -> list[dict]:
    """화면 영역(bbox) 내 판매점 조회"""
    rows = await pool.fetch("""
        SELECT id, store_id, name, address, phone, sido, sigungu, dong,
               sells_lotto, sells_pension, 
               sells_speetto_2000, sells_speetto_1000, sells_speetto_500,
               ST_Y(location) AS lat, ST_X(location) AS lng
        FROM stores
        WHERE is_active = TRUE
            AND is_online = FALSE
            AND location && ST_MakeEnvelope($1, $2, $3, $4, 4326)
        LIMIT $5
    """, min_lng, min_lat, max_lng, max_lat, limit)

    return [dict(r) for r in rows]