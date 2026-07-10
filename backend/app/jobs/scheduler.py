import logging
from datetime import datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.crawlers.lotto import crawl_latest_lotto_round, retry_lotto_sub_keys
from app.crawlers.pension import crawl_latest_pension_round, retry_pension_sub_keys
from app.crawlers.speetto import crawl_and_save_speetto
from app.crawlers.stores import crawl_all_stores, retry_stores_sub_keys
from app.crawlers.winning_stores import (
    crawl_all_winning_stores, retry_winning_sub_keys,
)
from app.crawlers.common import get_pending_bootstrap_failures
from app.jobs.saved_numbers_job import score_saved_numbers_job

logger = logging.getLogger(__name__)

KST = "Asia/Seoul"

SWEEP_JOB_ID = "sweep_failed"
SWEEP_INTERVAL = timedelta(minutes=5)

_scheduler: AsyncIOScheduler | None = None


async def sweep_failed() -> None:
    """모든 크롤러의 bootstrap_failures 잔여를 한 번씩 재시도.
    끝나면 SWEEP_INTERVAL 뒤로 self-reschedule."""
    logger.info("[SWEEP] 실패 작업 재시도 시작")
    try:
        for name, task_name, fn in (
            ("lotto",   "crawl_lotto",   retry_lotto_sub_keys),
            ("pension", "crawl_pension", retry_pension_sub_keys),
            ("stores",  "crawl_stores",  retry_stores_sub_keys),
            ("winning", "crawl_winning", retry_winning_sub_keys),
        ):
            try:
                pending = await get_pending_bootstrap_failures(task_name)
                if not pending:
                    continue
                logger.info(f"[SWEEP] {name}: {len(pending)}건 retry")
                await fn(pending)
            except Exception as e:
                logger.exception(f"[SWEEP] {name} 재시도 중 예외: {e}")
    finally:
        if _scheduler is not None:
            next_at = datetime.now() + SWEEP_INTERVAL
            try:
                _scheduler.reschedule_job(
                    SWEEP_JOB_ID,
                    trigger=DateTrigger(run_date=next_at, timezone=KST),
                )
                logger.info(f"[SWEEP] 다음 실행: {next_at.isoformat()}")
            except Exception as e:
                logger.exception(f"[SWEEP] reschedule 실패: {e}")


def _build_scheduler() -> AsyncIOScheduler:
    sched = AsyncIOScheduler(timezone=KST)

    # 로또 — 토요일 21시에 수집
    sched.add_job(
        crawl_latest_lotto_round,
        CronTrigger(day_of_week="sat", hour=21, minute=0, timezone=KST),
        id="crawl_lotto_latest",
        replace_existing=True,
    )

    # 저장 번호 채점 — 로또 수집 직후(토 21:10) 미채점분 일괄 대조
    sched.add_job(
        score_saved_numbers_job,
        CronTrigger(day_of_week="sat", hour=21, minute=10, timezone=KST),
        id="score_saved_numbers",
        replace_existing=True,
    )

    # 연금복권 — 목요일 21시에 수집
    sched.add_job(
        crawl_latest_pension_round,
        CronTrigger(day_of_week="thu", hour=21, minute=0, timezone=KST),
        id="crawl_pension_latest",
        replace_existing=True,
    )

    # 스피또 — 판매현황 1시간 간격
    sched.add_job(
        crawl_and_save_speetto,
        IntervalTrigger(hours=1),
        id="crawl_speetto",
        replace_existing=True,
    )

    # 판매점 — 매일 03:00
    sched.add_job(
        crawl_all_stores,
        CronTrigger(hour=3, minute=0, timezone=KST),
        id="crawl_stores",
        replace_existing=True,
    )

    # 당첨판매점 — 1시간 간격
    sched.add_job(
        crawl_all_winning_stores,
        IntervalTrigger(hours=1),
        id="crawl_winning_stores",
        replace_existing=True,
    )

    # 실패 sweeper — 첫 실행만 등록, 이후엔 job 내부에서 self-reschedule
    sched.add_job(
        sweep_failed,
        DateTrigger(run_date=datetime.now() + SWEEP_INTERVAL, timezone=KST),
        id=SWEEP_JOB_ID,
        replace_existing=True,
    )

    return sched


def start_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is not None:
        return _scheduler
    _scheduler = _build_scheduler()
    _scheduler.start()
    jobs = [(j.id, str(j.next_run_time)) for j in _scheduler.get_jobs()]
    logger.info(f"[SCHED] 시작: {len(jobs)}개 잡 등록 — {jobs}")
    return _scheduler


def shutdown_scheduler() -> None:
    global _scheduler
    if _scheduler is None:
        return
    _scheduler.shutdown(wait=False)
    _scheduler = None
    logger.info("[SCHED] 종료")


async def _run_forever() -> None:
    import asyncio
    import signal

    from app.core.database import close_pool

    start_scheduler()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    try:
        await stop_event.wait()
    finally:
        shutdown_scheduler()
        await close_pool()


if __name__ == "__main__":
    import asyncio

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.INFO)

    asyncio.run(_run_forever())