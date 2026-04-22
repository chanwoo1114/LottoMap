
import argparse
import asyncio
import logging

from app.core.config import settings
from app.core.database import get_pool
from app.crawlers.lotto import crawl_and_save_all_lotto_results
from app.crawlers.pension import crawl_and_save_all_pension_results
from app.crawlers.speetto import crawl_and_save_speetto
from app.crawlers.stores import crawl_all_stores
from app.crawlers.winning_stores import crawl_all_winning_stores

logger = logging.getLogger(__name__)

# step 이름 → 해당 테이블
STEP_TABLE = {
    "stores":  "stores",
    "lotto":   "lotto_results",
    "pension": "pension_results",
    "speetto": "speetto_games",
    "winning": "winning_stores",
}

DEFAULT_ORDER = ["speetto", "pension", "lotto", "stores", "winning"]


async def _has_data(pool, table: str) -> bool:
    row = await pool.fetchrow(f"SELECT EXISTS(SELECT 1 FROM {table})")
    return bool(row[0])


async def _run_step(step: str, args: argparse.Namespace) -> None:
    if step == "stores":
        await crawl_all_stores()
    elif step == "lotto":
        latest = args.lotto_latest or settings.LOTTO_LATEST
        if latest is None:
            raise ValueError(
                "lotto 스텝 실행에는 --lotto-latest 또는 .env의 LOTTO_LATEST 필요"
            )
        await crawl_and_save_all_lotto_results(latest)
    elif step == "pension":
        await crawl_and_save_all_pension_results()
    elif step == "speetto":
        await crawl_and_save_speetto()
    elif step == "winning":
        await crawl_all_winning_stores()


async def bootstrap(args: argparse.Namespace) -> dict:
    pool = await get_pool()
    steps = args.only or DEFAULT_ORDER
    force = set(args.force or [])

    report = {"ran": [], "skipped": [], "failed": []}

    for step in steps:
        table = STEP_TABLE[step]
        if step not in force and await _has_data(pool, table):
            logger.info(f"[SKIP] {step} ({table} 데이터 존재)")
            report["skipped"].append(step)
            continue

        logger.info(f"[RUN ] {step}")
        try:
            await _run_step(step, args)
            report["ran"].append(step)
        except Exception as e:
            logger.exception(f"[FAIL] {step}: {e}")
            report["failed"].append(step)
            if not args.continue_on_error:
                break

    logger.info(
        f"[BOOTSTRAP] ran={report['ran']}, "
        f"skipped={report['skipped']}, failed={report['failed']}"
    )
    return report


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
        description="초기 데이터 적재. 테이블이 비어있는 스텝만 실행.",
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
        "--continue-on-error", action="store_true",
        help="중간 스텝 실패해도 다음 스텝 진행",
    )
    return p


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(bootstrap(_build_parser().parse_args()))
