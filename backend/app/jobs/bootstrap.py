import argparse
import asyncio
import logging

from app.core.database import close_pool
from app.crawlers.common import delay
from app.crawlers.lotto import (
    crawl_and_save_all_lotto_results, fetch_latest_lotto_round,
)
from app.crawlers.pension import crawl_and_save_all_pension_results
from app.crawlers.speetto import crawl_and_save_speetto
from app.crawlers.stores import crawl_all_stores
from app.crawlers.winning_stores import crawl_all_winning_stores

logger = logging.getLogger(__name__)

DEFAULT_ORDER = [
    "speetto",
    "pension",
    "lotto",
    "stores",
    "winning",
]


async def _run_bulk_step(step: str, args: argparse.Namespace) -> dict:
    if step == "stores":
        return await crawl_all_stores()
    if step == "lotto":
        latest = args.lotto_latest
        if latest is None:
            latest = await fetch_latest_lotto_round()
        return await crawl_and_save_all_lotto_results(latest)
    if step == "pension":
        return await crawl_and_save_all_pension_results()
    if step == "speetto":
        return await crawl_and_save_speetto()
    if step == "winning":
        return await crawl_all_winning_stores()
    raise ValueError(f"알 수 없는 step: {step}")


async def bootstrap_bulk(args: argparse.Namespace) -> None:
    """모든 step bulk 1회씩 실행 후 종료.
    실패는 bootstrap_failures 테이블에 남고, 이후 scheduler의 sweep 잡이 재시도."""
    steps = args.only or DEFAULT_ORDER

    logger.info("=== bootstrap bulk 시작 ===")
    for i, step in enumerate(steps):
        if i > 0:
            logger.info(f"[{step}] step 전환 딜레이")
            await delay()

        logger.info(f"[{step}] 백필 시작")
        try:
            result = await _run_bulk_step(step, args)
            failures = result.get("failures", [])
            logger.info(f"[{step}] 백필 완료 (실패={len(failures)})")
        except Exception as e:
            logger.exception(f"[{step}] 백필 중 예외, 다음 step 진행: {e}")
    logger.info("=== bootstrap bulk 종료 ===")


def _parse_steps(value: str) -> list[str]:
    items = [s.strip() for s in (value or "").split(",") if s.strip()]
    unknown = [s for s in items if s not in DEFAULT_ORDER]
    if unknown:
        raise argparse.ArgumentTypeError(
            f"알 수 없는 스텝: {unknown}. 가능한 값: {DEFAULT_ORDER}"
        )
    return items


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="app.jobs.bootstrap",
        description="초기 데이터 적재 (bulk 1회 실행 후 종료). 실패는 scheduler가 재시도.",
    )
    p.add_argument(
        "--only", type=_parse_steps, default=None,
        help=f"실행할 스텝만 지정 (콤마 구분). 기본: 전체 {DEFAULT_ORDER}",
    )
    p.add_argument(
        "--lotto-latest", type=int, default=None,
        help="로또 최신 회차 override (생략 시 동행복권 페이지에서 자동 감지)",
    )
    return p


async def _async_main() -> None:
    args = _build_parser().parse_args()
    try:
        await bootstrap_bulk(args)
    finally:
        await close_pool()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(_async_main())