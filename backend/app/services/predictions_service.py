"""AI/Statistical 예측 저장·채점·조회 서비스."""
import logging

import asyncpg

logger = logging.getLogger(__name__)


INSERT_PREDICTION_SQL = """
INSERT INTO ai_predictions (
    target_round, model, strategy, numbers, confidence
) VALUES ($1, $2, $3, $4, $5)
"""


SCORE_SQL = """
UPDATE ai_predictions p
SET hit_count = sub.hits,
    matched_bonus = sub.bonus_hit,
    scored_at = NOW()
FROM (
    SELECT p.id,
           (SELECT COUNT(*)::smallint
              FROM unnest(p.numbers) AS n
             WHERE n IN (r.num1, r.num2, r.num3, r.num4, r.num5, r.num6)) AS hits,
           (r.bonus = ANY(p.numbers)) AS bonus_hit
    FROM ai_predictions p
    JOIN lotto_results r ON r.round_no = p.target_round
    WHERE p.target_round = $1 AND p.hit_count IS NULL
) sub
WHERE p.id = sub.id
"""


async def save_predictions(
    pool: asyncpg.Pool, target_round: int, items: list[dict]
) -> int:
    '''items: [{model, strategy, numbers, confidence}, ...]'''
    if not items:
        return 0
    rows = [
        (
            target_round,
            it["model"],
            it.get("strategy", ""),
            it["numbers"],
            int(it.get("confidence", 0)),
        )
        for it in items
    ]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(INSERT_PREDICTION_SQL, rows)
    logger.info(f"[PRED] target_round={target_round}, saved={len(rows)}")
    return len(rows)


async def score_round(pool: asyncpg.Pool, round_no: int) -> int:
    '''해당 회차의 미채점 예측을 실제 당첨번호와 비교해 hit_count/matched_bonus 업데이트'''
    result = await pool.execute(SCORE_SQL, round_no)
    updated = int(result.split()[-1])
    logger.info(f"[PRED] round={round_no} 채점 완료 ({updated}건)")
    return updated


async def list_recent_predictions(
    pool: asyncpg.Pool, rounds: int = 10
) -> list[dict]:
    '''최근 N개 target_round의 예측·채점 결과를 회차 단위로 묶어 반환'''
    rows = await pool.fetch(
        """
        WITH recent AS (
            SELECT DISTINCT target_round
            FROM ai_predictions
            ORDER BY target_round DESC
            LIMIT $1
        )
        SELECT
            p.id, p.target_round, p.model, p.strategy,
            p.numbers, p.confidence,
            p.hit_count, p.matched_bonus,
            p.created_at, p.scored_at,
            r.draw_date,
            r.num1, r.num2, r.num3, r.num4, r.num5, r.num6, r.bonus
        FROM ai_predictions p
        JOIN recent ON recent.target_round = p.target_round
        LEFT JOIN lotto_results r ON r.round_no = p.target_round
        ORDER BY p.target_round DESC, p.model, p.strategy, p.id
        """,
        rounds,
    )

    grouped: dict[int, dict] = {}
    for r in rows:
        tr = r["target_round"]
        if tr not in grouped:
            has_result = r["num1"] is not None
            grouped[tr] = {
                "target_round": tr,
                "draw_date": r["draw_date"],
                "winning_numbers": (
                    sorted([r["num1"], r["num2"], r["num3"],
                            r["num4"], r["num5"], r["num6"]])
                    if has_result else None
                ),
                "bonus": r["bonus"] if has_result else None,
                "predictions": [],
            }
        grouped[tr]["predictions"].append({
            "id": r["id"],
            "model": r["model"],
            "strategy": r["strategy"] or None,
            "numbers": list(r["numbers"]),
            "confidence": r["confidence"],
            "hit_count": r["hit_count"],
            "matched_bonus": r["matched_bonus"],
            "created_at": r["created_at"],
            "scored_at": r["scored_at"],
        })
    return [grouped[k] for k in sorted(grouped.keys(), reverse=True)]
