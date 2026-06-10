import asyncio
import logging
from typing import Any, Awaitable, Callable

import asyncpg

from app.services.generators.ai_predictor import AIGeneratorV3
from app.services.generators.pension import PensionGenerator
from app.services.generators.statistical import StatisticalGeneratorV3

logger = logging.getLogger(__name__)


class GeneratorCache:
    """생성기 인스턴스를 회차 단위로 캐싱·재학습 관리"""
    def __init__(self) -> None:
        """캐시 슬롯과 슬롯별 재학습 동기화 락 초기화"""
        self._statistical: StatisticalGeneratorV3 | None = None
        self._stat_round: int | None = None
        self._ai: AIGeneratorV3 | None = None
        self._ai_round: int | None = None
        self._pension: PensionGenerator | None = None
        self._pension_round: int | None = None
        self._stat_lock = asyncio.Lock()
        self._ai_lock = asyncio.Lock()
        self._pension_lock = asyncio.Lock()

    @staticmethod
    async def _max_round(pool: asyncpg.Pool, table: str) -> int | None:
        """주어진 테이블의 최신 회차 번호 조회"""
        row = await pool.fetchrow(f"SELECT MAX(round_no) AS r FROM {table}")
        return row["r"] if row else None

    @staticmethod
    async def _current_round(pool: asyncpg.Pool) -> int | None:
        """로또 최신 회차 번호 조회"""
        return await GeneratorCache._max_round(pool, "lotto_results")

    @staticmethod
    async def _current_pension_round(pool: asyncpg.Pool) -> int | None:
        """연금복권 최신 회차 번호 조회"""
        return await GeneratorCache._max_round(pool, "pension_results")

    async def _get_or_rebuild(
        self,
        lock: asyncio.Lock,
        cache_attr: str,
        round_attr: str,
        round_fn: Callable[[asyncpg.Pool], Awaitable[int | None]],
        factory: Callable[[], Any],
        build: Callable[[Any, asyncpg.Pool], Awaitable[None]],
        label: str,
        pool: asyncpg.Pool,
    ) -> Any:
        """슬롯 락 안에서 회차 비교 후 필요 시 재학습하여 캐시된 생성기 반환"""
        async with lock:
            cur = await round_fn(pool)
            if getattr(self, cache_attr) is None or getattr(self, round_attr) != cur:
                logger.info(f"[generator-cache] {label} 재학습 (round {getattr(self, round_attr)} -> {cur})")
                gen = factory()
                await build(gen, pool)
                setattr(self, cache_attr, gen)
                setattr(self, round_attr, cur)
            return getattr(self, cache_attr)

    async def get_statistical(self, pool: asyncpg.Pool) -> StatisticalGeneratorV3:
        """통계 생성기 반환(회차 변경 시 재학습)"""
        return await self._get_or_rebuild(
            self._stat_lock,
            "_statistical",
            "_stat_round",
            self._current_round,
            StatisticalGeneratorV3,
            lambda gen, p: gen.load_data(p),
            "Statistical",
            pool,
        )

    async def get_ai(self, pool: asyncpg.Pool) -> AIGeneratorV3:
        """AI 생성기 반환(회차 변경 시 재학습)"""
        return await self._get_or_rebuild(
            self._ai_lock,
            "_ai",
            "_ai_round",
            self._current_round,
            AIGeneratorV3,
            lambda gen, p: gen.train(p),
            "AI",
            pool,
        )

    async def get_pension(self, pool: asyncpg.Pool) -> PensionGenerator:
        """연금복권 생성기 반환(회차 변경 시 재학습)"""
        return await self._get_or_rebuild(
            self._pension_lock,
            "_pension",
            "_pension_round",
            self._current_pension_round,
            PensionGenerator,
            lambda gen, p: gen.load_data(p),
            "Pension",
            pool,
        )

    async def invalidate(self) -> None:
        """수동 무효화 (예: 크롤링 직후)"""
        async with self._stat_lock, self._ai_lock, self._pension_lock:
            self._statistical = None
            self._stat_round = None
            self._ai = None
            self._ai_round = None
            self._pension = None
            self._pension_round = None
            logger.info("[generator-cache] 캐시 무효화")


generator_cache = GeneratorCache()
