import logging

from app.core.database import get_pool
from app.services.generators.cache import generator_cache
from app.services.generators.statistical import StatisticalGeneratorV3
from app.services.predictions_service import (
    save_predictions, score_round, get_round_predictions,
)

logger = logging.getLogger(__name__)

AI_COUNT = 5
STAT_COUNT_PER_STRATEGY = 1


async def _latest_round(pool) -> int | None:
    row = await pool.fetchrow("SELECT MAX(round_no) AS r FROM lotto_results")
    return row["r"] if row else None


def _extract(items: list[dict], model: str, strategy: str = "") -> list[dict]:
    out: list[dict] = []
    for it in items or []:
        nums = it.get("numbers")
        if not nums or len(nums) != 6 or "error" in it:
            continue
        conf = it.get("confidence")
        if conf is None:
            conf = it.get("pattern_score", 0)
        out.append({
            "model": model,
            "strategy": strategy,
            "numbers": sorted(int(n) for n in nums),
            "confidence": int(conf or 0),
        })
    return out


async def generate_for_next_round() -> dict:
    logger.info("[START] generate_predictions")

    pool = await get_pool()
    last = await _latest_round(pool)
    if last is None:
        raise RuntimeError("lotto_results 비어있음 - 예측 생성 불가")
    target = last + 1

    all_items: list[dict] = []

    ai = await generator_cache.get_ai(pool)
    ai_out = await ai.generate(pool, count=AI_COUNT)
    all_items.extend(_extract(ai_out, "ai_ensemble"))

    stat = await generator_cache.get_statistical(pool)
    for strat in StatisticalGeneratorV3.STRATEGIES:
        stat_out = await stat.generate(pool, strategy=strat, count=STAT_COUNT_PER_STRATEGY)
        all_items.extend(_extract(stat_out, "statistical", strat))

    saved = await save_predictions(pool, target, all_items)
    logger.info(f"[END] generate_predictions: target={target}, saved={saved}")
    return {"target_round": target, "saved": saved}


async def score_latest_round() -> dict:
    logger.info("[START] score_predictions")

    pool = await get_pool()
    last = await _latest_round(pool)
    if last is None:
        raise RuntimeError("lotto_results 비어있음 - 채점 불가")

    updated = await score_round(pool, last)
    logger.info(f"[END] score_predictions: round={last}, scored={updated}")
    return {"round": last, "scored": updated}


_PRED_LOCK_NS = 7242


async def get_or_create_round_predictions(round_no: int) -> dict | None:
    """
    - 있으면: 추첨됐는데 미채점이면 채점 후 반환
    - 없고 '다음 회차(추첨 전)'면: 생성 후 반환
    - 과거인데 없으면: None
    """
    pool = await get_pool()

    existing = await get_round_predictions(pool, round_no)
    if existing is not None:
        # 추첨 끝났는데 미채점이면 채점 (멱등)
        if existing["draw_date"] is not None and any(
            p["hit_count"] is None for p in existing["predictions"]
        ):
            await score_round(pool, round_no)
            existing = await get_round_predictions(pool, round_no)
        return existing

    # 없음 — 다음 회차(추첨 전)일 때만 생성
    latest = await _latest_round(pool)
    if latest is None or round_no != latest + 1:
        return None

    # 동시 첫조회 직렬화 (레이스 → 중복 생성 방지)
    async with pool.acquire() as conn:
        await conn.execute("SELECT pg_advisory_lock($1, $2)", _PRED_LOCK_NS, round_no)
        try:
            if await get_round_predictions(pool, round_no) is None:
                await generate_for_next_round()      # latest+1 == round_no
            return await get_round_predictions(pool, round_no)
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1, $2)", _PRED_LOCK_NS, round_no)


if __name__ == "__main__":
    import argparse
    import asyncio

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    p = argparse.ArgumentParser(prog="predictions_job")
    p.add_argument("cmd", choices=["generate", "score"])
    args = p.parse_args()

    if args.cmd == "generate":
        asyncio.run(generate_for_next_round())
    else:
        asyncio.run(score_latest_round())
