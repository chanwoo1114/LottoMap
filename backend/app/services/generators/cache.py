import asyncio
import logging

import asyncpg

from app.services.generators.ai_predictor import AIGeneratorV3
from app.services.generators.pension import PensionGenerator
from app.services.generators.statistical import StatisticalGeneratorV3

logger = logging.getLogger(__name__)


class GeneratorCache:
    def __init__(self) -> None:
        self._statistical: StatisticalGeneratorV3 | None = None
        self._stat_round: int | None = None
        self._ai: AIGeneratorV3 | None = None
        self._ai_round: int | None = None
        self._pension: PensionGenerator | None = None
        self._pension_round: int | None = None
        self._lock = asyncio.Lock()

    @staticmethod
    async def _current_round(pool: asyncpg.Pool) -> int | None:
        row = await pool.fetchrow("SELECT MAX(round_no) AS r FROM lotto_results")
        return row["r"] if row else None

    @staticmethod
    async def _current_pension_round(pool: asyncpg.Pool) -> int | None:
        row = await pool.fetchrow("SELECT MAX(round_no) AS r FROM pension_results")
        return row["r"] if row else None

    async def get_statistical(self, pool: asyncpg.Pool) -> StatisticalGeneratorV3:
        async with self._lock:
            cur = await self._current_round(pool)
            if self._statistical is None or self._stat_round != cur:
                logger.info(f"[generator-cache] Statistical 재학습 (round {self._stat_round} -> {cur})")
                gen = StatisticalGeneratorV3()
                await gen.load_data(pool)
                self._statistical = gen
                self._stat_round = cur
            return self._statistical

    async def get_ai(self, pool: asyncpg.Pool) -> AIGeneratorV3:
        async with self._lock:
            cur = await self._current_round(pool)
            if self._ai is None or self._ai_round != cur:
                logger.info(f"[generator-cache] AI 재학습 (round {self._ai_round} -> {cur})")
                gen = AIGeneratorV3()
                await gen.train(pool)
                self._ai = gen
                self._ai_round = cur
            return self._ai

    async def get_pension(self, pool: asyncpg.Pool) -> PensionGenerator:
        async with self._lock:
            cur = await self._current_pension_round(pool)
            if self._pension is None or self._pension_round != cur:
                logger.info(f"[generator-cache] Pension 재학습 (round {self._pension_round} -> {cur})")
                gen = PensionGenerator()
                await gen.load_data(pool)
                self._pension = gen
                self._pension_round = cur
            return self._pension

    async def invalidate(self) -> None:
        """수동 무효화 (예: 크롤링 직후)"""
        async with self._lock:
            self._statistical = None
            self._stat_round = None
            self._ai = None
            self._ai_round = None
            self._pension = None
            self._pension_round = None
            logger.info("[generator-cache] 캐시 무효화")


generator_cache = GeneratorCache()