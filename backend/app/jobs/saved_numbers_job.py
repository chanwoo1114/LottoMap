import logging

from app.core.database import get_pool
from app.services import saved_numbers_service

logger = logging.getLogger(__name__)


async def score_saved_numbers_job() -> dict:
    """추첨이 끝난 회차의 미채점 저장 번호를 전체 유저에 대해 일괄 채점한다."""
    logger.info("[START] score_saved_numbers")
    pool = await get_pool()
    updated = await saved_numbers_service.score_saved_numbers(pool)
    logger.info(f"[END] score_saved_numbers: scored={updated}")
    return {"scored": updated}


if __name__ == "__main__":
    import asyncio

    from app.core.database import close_pool

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    async def _main() -> None:
        try:
            await score_saved_numbers_job()
        finally:
            await close_pool()

    asyncio.run(_main())
