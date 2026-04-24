"""APScheduler 기반 크롤러 스케쥴 + 실패 sweeper.

- 추첨 스케줄: 로또(토 20:35), 연금복권(목 20:00)
- 모든 cron은 KST 기준
- 실패 작업은 별도 sweeper가 1시간 간격으로 재시도
"""
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.crawlers.lotto import crawl_latest_lotto_round, retry_failed_lotto
from app.crawlers.pension import crawl_latest_pension_round, retry_failed_pension
from app.crawlers.speetto import crawl_and_save_speetto, retry_failed_speetto
from app.crawlers.stores import crawl_all_stores, retry_failed_stores
from app.crawlers.winning_stores import (
    crawl_all_winning_stores, retry_failed_winning_stores,
)
from app.jobs.predictions_job import generate_for_next_round, score_latest_round

logger = logging.getLogger(__name__)

KST = "Asia/Seoul"

_scheduler: AsyncIOScheduler | None = None


async def sweep_failed() -> None:
    """모든 크롤러의 failed 작업을 한 번씩 재시도"""
    logger.info("[SWEEP] 실패 작업 재시도 시작")
    for name, fn in (
        ("lotto", retry_failed_lotto),
        ("pension", retry_failed_pension),
        ("speetto", retry_failed_speetto),
        ("stores", retry_failed_stores),
        ("winning", retry_failed_winning_stores),
    ):
        try:
            await fn()
        except Exception as e:
            logger.exception(f"[SWEEP] {name} 재시도 중 예외: {e}")


def _build_scheduler() -> AsyncIOScheduler:
    sched = AsyncIOScheduler(timezone=KST)

    # 로또 — 토요일 20:35 추첨, 22:00 KST에 수집
    sched.add_job(
        crawl_latest_lotto_round,
        CronTrigger(day_of_week="sat", hour=22, minute=0, timezone=KST),
        id="crawl_lotto_latest",
        replace_existing=True,
    )

    # 연금복권 — 목요일 20:00 추첨, 21:00 KST에 수집
    sched.add_job(
        crawl_latest_pension_round,
        CronTrigger(day_of_week="thu", hour=21, minute=0, timezone=KST),
        id="crawl_pension_latest",
        replace_existing=True,
    )

    # 스피또 — 판매현황은 매일 갱신 필요, 새벽 04:00 KST
    sched.add_job(
        crawl_and_save_speetto,
        CronTrigger(hour=4, minute=0, timezone=KST),
        id="crawl_speetto",
        replace_existing=True,
    )

    # 판매점 — 변경 빈도 낮음. 일요일 03:00 KST
    sched.add_job(
        crawl_all_stores,
        CronTrigger(day_of_week="sun", hour=3, minute=0, timezone=KST),
        id="crawl_stores",
        replace_existing=True,
    )

    # 당첨판매점 — 일요일 04:00 KST (stores 이후)
    sched.add_job(
        crawl_all_winning_stores,
        CronTrigger(day_of_week="sun", hour=4, minute=0, timezone=KST),
        id="crawl_winning_stores",
        replace_existing=True,
    )

    # 예측 채점 — 토요일 23:00 KST (로또 수집 이후)
    sched.add_job(
        score_latest_round,
        CronTrigger(day_of_week="sat", hour=23, minute=0, timezone=KST),
        id="score_predictions",
        replace_existing=True,
    )

    # 다음 회차 예측 생성 — 일요일 05:00 KST
    sched.add_job(
        generate_for_next_round,
        CronTrigger(day_of_week="sun", hour=5, minute=0, timezone=KST),
        id="generate_predictions",
        replace_existing=True,
    )

    # 실패 sweeper — 1시간 간격
    sched.add_job(
        sweep_failed,
        IntervalTrigger(hours=1),
        id="sweep_failed",
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