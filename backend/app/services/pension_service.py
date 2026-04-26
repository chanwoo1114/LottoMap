import asyncpg

from app.schema.pension_schema import PensionResultsQuery


_BASE_SELECT = """
    SELECT
        round_no, draw_date,
        first_prize_group, first_prize_number, bonus_number
    FROM pension_results
"""


async def search_pension_results(
    pool: asyncpg.Pool, q: PensionResultsQuery
) -> list[dict]:
    """회차 범위 + 페이지네이션. 최신 회차 우선."""
    conditions: list[str] = []
    params: list = []
    idx = 1

    if q.from_round is not None:
        conditions.append(f"round_no >= ${idx}")
        params.append(q.from_round)
        idx += 1

    if q.to_round is not None:
        conditions.append(f"round_no <= ${idx}")
        params.append(q.to_round)
        idx += 1

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.extend([q.size, (q.page - 1) * q.size])

    rows = await pool.fetch(
        f"""
        {_BASE_SELECT}
        {where}
        ORDER BY round_no DESC
        LIMIT ${idx} OFFSET ${idx + 1}
        """,
        *params,
    )
    return [dict(r) for r in rows]


async def get_pension_result_by_round(
    pool: asyncpg.Pool, round_no: int
) -> dict | None:
    row = await pool.fetchrow(f"{_BASE_SELECT} WHERE round_no = $1", round_no)
    return dict(row) if row else None


async def get_latest_pension_result(pool: asyncpg.Pool) -> dict | None:
    row = await pool.fetchrow(
        f"{_BASE_SELECT} ORDER BY round_no DESC LIMIT 1"
    )
    return dict(row) if row else None
