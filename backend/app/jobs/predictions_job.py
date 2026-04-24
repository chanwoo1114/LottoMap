import logging

from app.core.database import get_pool
from app.crawlers.common import log_crawl_start, log_crawl_finish
from app.services.generators.cache import generator_cache
from app.services.generators.statistical import StatisticalGeneratorV3
from app.services.predictions_service import save_predictions, score_round

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
    log_id = await log_crawl_start("generate_predictions")
    logger.info("[START] generate_predictions")

    try:
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
        msg = f"target={target}, saved={saved}"
        await log_crawl_finish(log_id, "success", msg)
        logger.info(f"[END] generate_predictions: {msg}")
        return {"target_round": target, "saved": saved}
    except Exception as e:
        await log_crawl_finish(log_id, "failed", str(e))
        logger.exception(f"[FAIL] generate_predictions: {e}")
        raise


async def score_latest_round() -> dict:
    log_id = await log_crawl_start("score_predictions")
    logger.info("[START] score_predictions")

    try:
        pool = await get_pool()
        last = await _latest_round(pool)
        if last is None:
            raise RuntimeError("lotto_results 비어있음 - 채점 불가")

        updated = await score_round(pool, last)
        msg = f"round={last}, scored={updated}"
        status = "success" if updated > 0 else "partial"
        await log_crawl_finish(log_id, status, msg)
        logger.info(f"[END] score_predictions: {msg}")
        return {"round": last, "scored": updated}
    except Exception as e:
        await log_crawl_finish(log_id, "failed", str(e))
        logger.exception(f"[FAIL] score_predictions: {e}")
        raise


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
