import logging
import re
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

from app.core.database import get_pool
from app.crawlers.common import BASE_URL, delay, get_client, log_crawl_task

logger = logging.getLogger(__name__)

DATE_RE = re.compile(r"(\d{4})[년.\-/\s]+(\d{1,2})[월.\-/\s]+(\d{1,2})")
GROUP_RE = re.compile(r"([1-5])\s*조")
SIX_DIGIT_RE = re.compile(r"(?<!\d)(\d{6})(?!\d)")


async def crawl_pension_round(
    round_no: int, client: httpx.AsyncClient | None = None
) -> dict | None:
    c = client or await get_client()

    try:
        resp = await c.get(f"{BASE_URL}/gameResult.do", params={
            "method": "win720",
            "Round": round_no,
        })
        html = resp.text
    except Exception as e:
        logger.error(f"[연금] round={round_no} 요청 실패: {e}")
        return None

    try:
        soup = BeautifulSoup(html, "html.parser")

        # 당첨 결과 컨테이너: win720 페이지 기준
        result_box = soup.select_one("div.win720_num") or soup.select_one("div.win_result") or soup
        text = result_box.get_text(" ", strip=True)

        # 추첨일
        m = DATE_RE.search(text)
        if not m:
            logger.warning(f"[연금] round={round_no} 추첨일 파싱 실패")
            return None
        draw_date = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()

        # 1등 조 (보통 본문에 '○조' 형태로 표기)
        group_match = GROUP_RE.search(text)
        if not group_match:
            logger.warning(f"[연금] round={round_no} 조 파싱 실패")
            return None
        first_prize_group = int(group_match.group(1))

        # 6자리 숫자들: 보통 [1등번호, 보너스번호] 순서
        six_digits = SIX_DIGIT_RE.findall(text)
        if len(six_digits) < 2:
            logger.warning(f"[연금] round={round_no} 6자리 번호 부족: {six_digits}")
            return None

        first_prize_number = six_digits[0]
        bonus_number = six_digits[1]

        return {
            "round_no": round_no,
            "draw_date": draw_date,
            "first_prize_group": first_prize_group,
            "first_prize_number": first_prize_number,
            "bonus_number": bonus_number,
        }
    except Exception as e:
        logger.error(f"[연금] round={round_no} 파싱 실패: {e}")
        return None


async def save_pension_results_to_db(results: list[dict]) -> int:
    if not results:
        return 0

    pool = await get_pool()
    query = """
        INSERT INTO pension_results (
            round_no, draw_date,
            first_prize_group, first_prize_number, bonus_number
        ) VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (round_no) DO NOTHING
    """
    rows = [
        (
            r["round_no"], r["draw_date"],
            r["first_prize_group"], r["first_prize_number"], r["bonus_number"],
        )
        for r in results
    ]

    async with pool.acquire() as conn:
        await conn.executemany(query, rows)

    logger.info(f"[DB] 연금 {len(rows)}건 저장 시도 (중복 제외)")
    return len(rows)


async def find_missing_pension_rounds(latest_round: int, start_round: int = 1) -> list[int]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT round_no FROM pension_results WHERE round_no BETWEEN $1 AND $2",
        start_round, latest_round,
    )
    existing = {r["round_no"] for r in rows}
    expected = set(range(start_round, latest_round + 1))
    return sorted(expected - existing)


async def crawl_and_save_all_pension_results(latest_round: int, start_round: int = 1) -> int:
    """1~latest_round 전체 백필. 회차별 단일 결과라 매 회차 호출."""
    client = await get_client()
    total = 0

    try:
        for n in range(start_round, latest_round + 1):
            result = await crawl_pension_round(n, client=client)
            if result:
                total += await save_pension_results_to_db([result])
            await delay()
    finally:
        await client.aclose()

    logger.info(f"[연금 크롤링 완료] 시도 건수 {total}")

    missing = await find_missing_pension_rounds(latest_round, start_round)
    if missing:
        for round_no in missing:
            await log_crawl_task("crawl_pension_round", "failed", str(round_no))
        logger.warning(f"[연금] 누락 {len(missing)}건 crawl_logs 기록: {missing}")
    else:
        await log_crawl_task(
            "crawl_pension_all", "success", f"range={start_round}~{latest_round}"
        )

    return total


async def fill_missing_pension_rounds(latest_round: int, start_round: int = 1) -> int:
    """pension_results에서 누락된 회차만 재크롤링·저장하고 crawl_logs에 기록"""
    missing = await find_missing_pension_rounds(latest_round, start_round)
    if not missing:
        logger.info(f"[연금 보강] 누락 회차 없음 ({start_round}~{latest_round})")
        return 0

    logger.info(f"[연금 보강] 누락 {len(missing)}건: {missing}")

    saved_total = 0
    client = await get_client()
    try:
        for n in missing:
            result = await crawl_pension_round(n, client=client)
            if result:
                saved_total += await save_pension_results_to_db([result])
            await delay()
    finally:
        await client.aclose()

    still_missing = await find_missing_pension_rounds(latest_round, start_round)
    filled = sorted(set(missing) - set(still_missing))

    for round_no in filled:
        await log_crawl_task("crawl_pension_round", "success", str(round_no))
    for round_no in still_missing:
        await log_crawl_task("crawl_pension_round", "failed", str(round_no))

    if still_missing:
        logger.warning(f"[연금 보강] 여전히 누락 {len(still_missing)}건: {still_missing}")
    else:
        logger.info(f"[연금 보강] 전 회차 완료 ({start_round}~{latest_round})")

    return saved_total
