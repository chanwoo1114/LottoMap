from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.database import get_pool
from app.schema.generator_schema import (
    AIQuery, GeneratorResponse, PensionQuery, StatisticalQuery,
)
from app.services.generators.cache import generator_cache

router = APIRouter(prefix="/generator", tags=["번호 생성"])


@router.get(
    "/statistical",
    response_model=GeneratorResponse,
    summary="로또 - 통계 기반 번호 생성",
)
async def statistical_numbers(
    q: Annotated[StatisticalQuery, Query()],
    pool: asyncpg.Pool = Depends(get_pool),
):
    """전략: hot/cold/balanced/overdue/pattern_match/contrarian/streak_based"""
    gen = await generator_cache.get_statistical(pool)
    results = await gen.generate(
        pool,
        strategy=q.strategy,
        count=q.count,
        exclude_numbers=q.exclude,
        include_numbers=q.include,
    )
    if results and "error" in results[0]:
        raise HTTPException(503, results[0]["error"])
    return {"results": results, "count": len(results)}


@router.get(
    "/ai",
    response_model=GeneratorResponse,
    summary="로또 - AI 앙상블 번호 생성",
)
async def ai_numbers(
    q: Annotated[AIQuery, Query()],
    pool: asyncpg.Pool = Depends(get_pool),
):
    """7개 모델(마르코프·사이클·클러스터·트렌드·포지션·갭가속·연번) 앙상블 + 몬테카를로 샘플링"""
    gen = await generator_cache.get_ai(pool)
    results = await gen.generate(
        pool,
        count=q.count,
        temperature=q.temperature,
        exclude_numbers=q.exclude,
        include_numbers=q.include,
    )
    if results and "error" in results[0]:
        raise HTTPException(503, results[0]["error"])
    return {"results": results, "count": len(results)}


@router.get(
    "/statistical/analysis",
    summary="로또 - 통계 분석 덤프",
)
async def statistical_analysis(
    pool: asyncpg.Pool = Depends(get_pool),
):
    """번호별 온도(hot/warm/cold/ice), 연체 번호, PMI 상위 페어, 연속 출현 번호 등"""
    gen = await generator_cache.get_statistical(pool)
    return await gen.get_full_analysis(pool)


@router.get(
    "/ai/insight",
    summary="로또 - AI 학습 인사이트",
)
async def ai_insight(
    pool: asyncpg.Pool = Depends(get_pool),
):
    """모델 가중치, 백테스트 히트율, 추세 상승/하락 번호, 연체 번호 등"""
    gen = await generator_cache.get_ai(pool)
    return await gen.get_full_insight(pool)


@router.get(
    "/pension",
    response_model=GeneratorResponse,
    summary="연금복권 - 번호 생성",
)
async def pension_numbers(
    q: Annotated[PensionQuery, Query()],
    pool: asyncpg.Pool = Depends(get_pool),
):
    """조 1~5 + 6자리 숫자. 전략: hot/cold/balanced/random, fixed_group으로 조 고정 가능"""
    gen = await generator_cache.get_pension(pool)
    results = await gen.generate(
        pool,
        strategy=q.strategy,
        count=q.count,
        fixed_group=q.fixed_group,
    )
    if results and "error" in results[0]:
        raise HTTPException(503, results[0]["error"])
    return {"results": results, "count": len(results)}


@router.get(
    "/pension/analysis",
    summary="연금복권 - 자리별 분석",
)
async def pension_analysis(
    pool: asyncpg.Pool = Depends(get_pool),
):
    """조 1~5 분포, 각 자리(1~6)별 0~9 숫자 빈도 (전체/최근 30회)"""
    gen = await generator_cache.get_pension(pool)
    return await gen.get_analysis(pool)