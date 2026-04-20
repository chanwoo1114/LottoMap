async def save_stores_to_db(stores: list[dict]) -> int:
    if not stores:
        return 0

    pool = await get_pool()
    query = """
        INSERT INTO stores (
            store_id, name, phone, address, address_detail,
            sido, sigungu, dong, location,
            sells_lotto, sells_pension,
            sells_speetto_2000, sells_speetto_1000, sells_speetto_500
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, ST_SetSRID(ST_MakePoint($9, $10), 4326),
            $11, $12, $13, $14, $15
        )
        ON CONFLICT (store_id) DO UPDATE SET
            name               = EXCLUDED.name,
            phone              = EXCLUDED.phone,
            address            = EXCLUDED.address,
            address_detail     = EXCLUDED.address_detail,
            sido               = EXCLUDED.sido,
            sigungu            = EXCLUDED.sigungu,
            dong               = EXCLUDED.dong,
            location           = EXCLUDED.location,
            sells_lotto        = EXCLUDED.sells_lotto,
            sells_pension      = EXCLUDED.sells_pension,
            sells_speetto_2000 = EXCLUDED.sells_speetto_2000,
            sells_speetto_1000 = EXCLUDED.sells_speetto_1000,
            sells_speetto_500  = EXCLUDED.sells_speetto_500,
            is_active          = TRUE,
            updated_at         = NOW()
    """

    rows = [
        (
            s["store_id"], s["name"], s["phone"], s["address"], s["address_detail"],
            s["sido"], s["sigungu"], s["dong"],
            float(s["lon"]) if s["lon"] else 0.0,
            float(s["lat"]) if s["lat"] else 0.0,
            s["sells_lotto"], s["sells_pension"],
            s["sells_speetto_2000"], s["sells_speetto_1000"], s["sells_speetto_500"],
        )
        for s in stores
    ]

    async with pool.acquire() as conn:
        await conn.executemany(query, rows)

    logger.info(f"[DB] {len(rows)}건 저장 완료")
    return len(rows)

async def crawl_and_save_all_stores():
    client = await _get_client()
    total = 0

    try:
        for sido, sigungu in ALL_REGION_PAIRS:
            stores = await crawl_store_location_by_region(sido, sigungu, client=client)
            if stores:
                saved = await save_stores_to_db(stores)
                total += saved
    finally:
        await client.aclose()

    logger.info(f"[크롤링 완료] 전체 {total}건 저장")
    return total