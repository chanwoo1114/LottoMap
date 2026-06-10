import asyncpg

from app.schema.lotto_schema import LottoResultsQuery
from app.services import _round_results


_BASE_SELECT = """
    SELECT
        round_no, draw_date,
        ARRAY[num1, num2, num3, num4, num5, num6] AS numbers,
        bonus,
        first_prize_amount, first_prize_winners, total_sales
    FROM lotto_results
"""


async def search_lotto_results(
    pool: asyncpg.Pool, q: LottoResultsQuery
) -> list[dict]:
    """회차 범위 + 페이지네이션. 최신 회차 우선."""
    return await _round_results.search_round_results(
        pool, _BASE_SELECT, q.from_round, q.to_round, q.page, q.size
    )


async def get_lotto_result_by_round(
    pool: asyncpg.Pool, round_no: int
) -> dict | None:
    """특정 회차의 로또 당첨 결과 조회."""
    return await _round_results.get_by_round(pool, _BASE_SELECT, round_no)


async def get_latest_lotto_result(pool: asyncpg.Pool) -> dict | None:
    """가장 최신 회차의 로또 당첨 결과 조회."""
    return await _round_results.get_latest(pool, _BASE_SELECT)
