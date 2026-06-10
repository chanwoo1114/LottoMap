import asyncpg


async def search_round_results(
    pool: asyncpg.Pool,
    base_select: str,
    from_round,
    to_round,
    page: int,
    size: int,
) -> list[dict]:
    conditions: list[str] = []
    params: list = []
    idx = 1

    if from_round is not None:
        conditions.append(f"round_no >= ${idx}")
        params.append(from_round)
        idx += 1

    if to_round is not None:
        conditions.append(f"round_no <= ${idx}")
        params.append(to_round)
        idx += 1

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.extend([size, (page - 1) * size])

    rows = await pool.fetch(
        f"""
        {base_select}
        {where}
        ORDER BY round_no DESC
        LIMIT ${idx} OFFSET ${idx + 1}
        """,
        *params,
    )
    return [dict(r) for r in rows]


async def get_by_round(
    pool: asyncpg.Pool, base_select: str, round_no: int
) -> dict | None:
    row = await pool.fetchrow(f"{base_select} WHERE round_no = $1", round_no)
    return dict(row) if row else None


async def get_latest(pool: asyncpg.Pool, base_select: str) -> dict | None:
    row = await pool.fetchrow(
        f"{base_select} ORDER BY round_no DESC LIMIT 1"
    )
    return dict(row) if row else None
