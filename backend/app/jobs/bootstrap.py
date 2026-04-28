import argparse
import asyncio
import logging

from app.core.config import settings
from app.core.database import close_pool
from app.crawlers.common import (
    delay,
    get_pending_bootstrap_failures,
)
from app.crawlers.lotto import (
    crawl_and_save_all_lotto_results, retry_lotto_sub_keys,
)
from app.crawlers.pension import (
    crawl_and_save_all_pension_results, retry_pension_sub_keys,
)
from app.crawlers.speetto import (
    crawl_and_save_speetto, retry_speetto_sub_keys,
)
from app.crawlers.stores import (
    crawl_all_stores, retry_stores_sub_keys,
)
from app.crawlers.winning_stores import (
    crawl_all_winning_stores, retry_winning_sub_keys,
)

logger = logging.getLogger(__name__)

DEFAULT_ORDER = [
    "speetto",
    "pension",
    "lotto",
    "stores",
    "winning",
]

STEP_TASK = {
    "speetto": "crawl_speetto",
    "pension": "crawl_pension",
    "lotto":   "crawl_lotto",
    "stores":  "crawl_stores",
    "winning": "crawl_winning",
}

STEP_RETRY = {
    "speetto": retry_speetto_sub_keys,
    "pension": retry_pension_sub_keys,
    "lotto":   retry_lotto_sub_keys,
    "stores":  retry_stores_sub_keys,
    "winning": retry_winning_sub_keys,
}


async def _run_bulk_step(step: str, args: argparse.Namespace) -> dict:
    """step 일괄 백필. 크롤러 리턴 dict ({"failures": [...], ...})"""
    if step == "stores":
        return await crawl_all_stores()
    if step == "lotto":
        latest = args.lotto_latest or settings.LOTTO_LATEST
        if latest is None:
            raise ValueError(
                "lotto 스텝 실행에는 --lotto-latest 또는 .env의 LOTTO_LATEST 필요"
            )
        return await crawl_and_save_all_lotto_results(latest)
    if step == "pension":
        return await crawl_and_save_all_pension_results()
    if step == "speetto":
        return await crawl_and_save_speetto()
    if step == "winning":
        return await crawl_all_winning_stores()
    raise ValueError(f"알 수 없는 step: {step}")


async def _bulk_phase(args: argparse.Namespace) -> None:
    """Phase 1: 모든 step bulk 1회씩 실행. 한 step이 실패해도 다음 step 계속 진행."""
    steps = args.only or DEFAULT_ORDER

    logger.info("=== Phase 1: 전체 bulk 백필 ===")
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


async def _retry_phase(args: argparse.Namespace) -> None:
    """Phase 2: 모든 task의 bootstrap_failures 잔여를 함께 max_cycles회 재시도."""
    steps = args.only or DEFAULT_ORDER

    logger.info("=== Phase 2: 통합 retry 루프 ===")
    for cycle in range(1, args.max_cycles + 1):
        any_pending = False

        for step in steps:
            task_name = STEP_TASK[step]
            pending = await get_pending_bootstrap_failures(task_name)
            if not pending:
                continue
            any_pending = True

            logger.info(
                f"[{step}] cycle {cycle}/{args.max_cycles}: {len(pending)}건 retry"
            )
            try:
                result = await STEP_RETRY[step](pending)
                logger.info(
                    f"[{step}] cycle {cycle}: "
                    f"resolved={len(result.get('resolved', []))}, "
                    f"still_failed={len(result.get('still_failed', []))}"
                )
            except Exception as e:
                logger.exception(f"[{step}] retry 중 예외: {e}")

        if not any_pending:
            logger.info(f"cycle {cycle}: 모든 task 잔여 0 → retry 종료")
            return

        if cycle >= args.max_cycles:
            logger.warning(
                f"max_cycles({args.max_cycles}) 도달, 잔여 있음 → retry 종료"
            )
            return

        logger.info(f"{args.retry_interval}초 후 cycle {cycle+1}")
        await asyncio.sleep(args.retry_interval)


async def _final_summary(args: argparse.Namespace) -> None:
    """Phase 3: step별 최종 잔여 로그."""
    steps = args.only or DEFAULT_ORDER
    logger.info("=== Phase 3: 최종 요약 ===")
    for step in steps:
        task_name = STEP_TASK[step]
        remaining = len(await get_pending_bootstrap_failures(task_name))
        logger.info(f"[{step}] 최종 잔여={remaining}")


async def bootstrap_with_retry(args: argparse.Namespace) -> None:
    """전체 bulk 백필 후 통합 retry 루프. 완료/한도 도달 시 exit 0."""
    await _bulk_phase(args)
    await _retry_phase(args)
    await _final_summary(args)


def _parse_steps(value: str) -> list[str]:
    items = [s.strip() for s in (value or "").split(",") if s.strip()]
    unknown = [s for s in items if s not in STEP_TASK]
    if unknown:
        raise argparse.ArgumentTypeError(
            f"알 수 없는 스텝: {unknown}. 가능한 값: {list(STEP_TASK)}"
        )
    return items


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="app.jobs.bootstrap",
        description="초기 데이터 적재 (전체 bulk → 통합 retry 루프, 완료 시 exit 0).",
    )
    p.add_argument(
        "--only", type=_parse_steps, default=None,
        help=f"실행할 스텝만 지정 (콤마 구분). 기본: 전체 {DEFAULT_ORDER}",
    )
    p.add_argument(
        "--lotto-latest", type=int, default=None,
        help="로또 최신 회차 (lotto 스텝 실행 시 필수)",
    )
    p.add_argument(
        "--max-cycles", type=int, default=24,
        help="retry 사이클 한도 (기본 24 = retry-interval 1시간 × 24 = 24시간)",
    )
    p.add_argument(
        "--retry-interval", type=int, default=3600,
        help="retry 사이클 간격(초). 기본 3600 = 1시간",
    )
    return p


async def _async_main() -> None:
    args = _build_parser().parse_args()
    try:
        await bootstrap_with_retry(args)
    finally:
        await close_pool()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(_async_main())