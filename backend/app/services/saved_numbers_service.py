import logging

import asyncpg

logger = logging.getLogger(__name__)

# 미채점(matched_rank IS NULL) + 이미 추첨된 회차(lotto_results 존재)만 대조.
# 등수: 6=1등, 5+보너스=2등, 5=3등, 4=4등, 3=5등, 그 외=0(낙첨).
_SCORE_SQL = """
UPDATE user_lotto_numbers u
SET matched_count = sub.hits,
    matched_bonus = sub.bonus_hit,
    matched_rank  = CASE
        WHEN sub.hits = 6 THEN 1
        WHEN sub.hits = 5 AND sub.bonus_hit THEN 2
        WHEN sub.hits = 5 THEN 3
        WHEN sub.hits = 4 THEN 4
        WHEN sub.hits = 3 THEN 5
        ELSE 0 END,
    scored_at = NOW()
FROM (
    SELECT u.id,
           (SELECT COUNT(*)::smallint
              FROM unnest(u.numbers) AS n
             WHERE n IN (r.num1, r.num2, r.num3, r.num4, r.num5, r.num6)) AS hits,
           (r.bonus = ANY(u.numbers)) AS bonus_hit
    FROM user_lotto_numbers u
    JOIN lotto_results r ON r.round_no = u.round_no
    WHERE u.matched_rank IS NULL {user_filter}
) sub
WHERE u.id = sub.id
"""


async def score_saved_numbers(pool: asyncpg.Pool, user_id: int | None = None) -> int:
    """추첨이 끝난 회차의 미채점 저장 번호를 당첨번호와 대조해 등수까지 기록한다.

    user_id 지정 시 해당 유저만, 생략 시 전체(스케줄러 일괄 채점). 멱등하다."""
    if user_id is None:
        result = await pool.execute(_SCORE_SQL.format(user_filter=""))
    else:
        result = await pool.execute(_SCORE_SQL.format(user_filter="AND u.user_id = $1"), user_id)

    updated = int(result.split()[-1])
    if updated:
        logger.info(f"[SAVED] 채점 완료 {updated}건 (user={user_id or 'ALL'})")
    return updated


async def list_my_numbers(pool: asyncpg.Pool, user_id: int) -> list[dict]:
    """내가 저장한 로또 번호 목록 (최신순). 조회 시점에 미채점분을 먼저 채점한다.

    추첨이 끝난 회차는 당첨번호(winning_numbers/bonus)와 추첨일도 함께 내려준다."""
    await score_saved_numbers(pool, user_id)
    rows = await pool.fetch(
        """
        SELECT u.id, u.round_no, u.numbers, u.source,
               u.matched_count, u.matched_bonus, u.matched_rank, u.memo, u.created_at,
               r.draw_date,
               CASE WHEN r.round_no IS NOT NULL
                    THEN ARRAY[r.num1, r.num2, r.num3, r.num4, r.num5, r.num6]
               END AS winning_numbers,
               r.bonus AS winning_bonus
        FROM user_lotto_numbers u
        LEFT JOIN lotto_results r ON r.round_no = u.round_no
        WHERE u.user_id = $1
        ORDER BY u.created_at DESC
        """,
        user_id,
    )
    return [dict(r) for r in rows]


async def add_number(
    pool: asyncpg.Pool, user_id: int, numbers: list[int], source: str, memo: str
) -> dict:
    """번호 저장. 대상 회차는 '가장 최근 추첨 회차 + 1'(다음 추첨분)로 잡는다."""
    latest = await pool.fetchval("SELECT MAX(round_no) FROM lotto_results")
    next_round = (latest or 0) + 1

    row = await pool.fetchrow(
        f"""INSERT INTO user_lotto_numbers (user_id, round_no, numbers, source, memo)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, round_no, numbers, source,
                      matched_count, matched_bonus, matched_rank, memo, created_at""",
        user_id, next_round, numbers, source, memo,
    )
    return dict(row)


async def remove_number(pool: asyncpg.Pool, user_id: int, number_id: int) -> None:
    """저장 번호 삭제 (본인 것만)"""
    await pool.execute(
        "DELETE FROM user_lotto_numbers WHERE id = $1 AND user_id = $2",
        number_id, user_id,
    )
