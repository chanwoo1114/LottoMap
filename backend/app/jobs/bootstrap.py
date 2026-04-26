import argparse
import asyncio
import logging

from app.core.config import settings
from app.core.database import close_pool, get_pool
from app.crawlers.common import (
    delay,
    get_pending_bootstrap_failures,
    insert_bootstrap_failure,
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

STEP_TABLE = {
    "stores":  "stores",
    "lotto":   "lotto_results",
    "pension": "pension_results",
    "speetto": "speetto_games",
    "winning": "winning_stores",
}

DEFAULT_ORDER = [
    # "speetto",
    # "pension",
    # "lotto",
    # "stores",
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


async def _has_data(pool, table: str) -> bool:
    row = await pool.fetchrow(f"SELECT EXISTS(SELECT 1 FROM {table})")
    return bool(row[0])


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


async def _record_failures(task_name: str, failures: list[str]) -> None:
    for sub_key in failures:
        await insert_bootstrap_failure(task_name, sub_key)


async def _bulk_phase(args: argparse.Namespace) -> None:
    """Phase 1: 모든 step bulk 1회씩 실행."""
    pool = await get_pool()
    steps = args.only or DEFAULT_ORDER
    force = set(args.force or [])

    logger.info("=== Phase 1: 전체 bulk 백필 ===")
    for i, step in enumerate(steps):
        if i > 0:
            logger.info(f"[{step}] step 전환 딜레이")
            await delay()

        table = STEP_TABLE[step]
        task_name = STEP_TASK[step]
        print(table, task_name)
        if step not in force and await _has_data(pool, table):
            logger.info(f"[SKIP] {step} ({table} 데이터 존재)")
            continue

        logger.info(f"[{step}] 백필 시작")
        try:
            result = await _run_bulk_step(step, args)
            failures = result.get("failures", [])
            if failures:
                await _record_failures(task_name, failures)
            logger.info(f"[{step}] 백필 완료 (실패={len(failures)})")
        except Exception as e:
            logger.exception(f"[{step}] 백필 중 예외: {e}")
            if not args.continue_on_error:
                logger.error(
                    f"[{step}] 치명적 실패, --continue-on-error 없이 중단"
                )
                return


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
    breakpoint()
    await _retry_phase(args)
    breakpoint()
    await _final_summary(args)


def _parse_steps(value: str) -> list[str]:
    items = [s.strip() for s in (value or "").split(",") if s.strip()]
    unknown = [s for s in items if s not in STEP_TABLE]
    if unknown:
        raise argparse.ArgumentTypeError(
            f"알 수 없는 스텝: {unknown}. 가능한 값: {list(STEP_TABLE)}"
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
        "--force", type=_parse_steps, default=None,
        help="데이터 있어도 강제 실행할 스텝 (콤마 구분)",
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
    p.add_argument(
        "--continue-on-error", action="store_true",
        help="bulk 중 예외 발생해도 다음 스텝 진행",
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