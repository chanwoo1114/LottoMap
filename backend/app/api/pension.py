from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, Query

from app.api._helpers import or_404
from app.core.database import get_pool
from app.schema.pension_schema import (
    PensionResultResponse, PensionResultsQuery,
)
from app.services import pension_service

router = APIRouter(prefix="/pension", tags=["연금복권"])


@router.get(
    "/results",
    response_model=list[PensionResultResponse],
    summary="연금복권 회차 목록",
)
async def list_pension_results(
    q: Annotated[PensionResultsQuery, Query()],
    pool: asyncpg.Pool = Depends(get_pool),
):
    """회차 범위(`from_round`, `to_round`)로 필터링. 최신 회차 우선."""
    return await pension_service.search_pension_results(pool, q)


@router.get(
    "/results/latest",
    response_model=PensionResultResponse,
    summary="연금복권 최신 회차",
)
async def get_latest_pension(
    pool: asyncpg.Pool = Depends(get_pool),
):
    """가장 최근 회차 한 건. 데이터 없으면 404."""
    return or_404(await pension_service.get_latest_pension_result(pool), "연금복권 결과가 없습니다.")


@router.get(
    "/results/{round_no}",
    response_model=PensionResultResponse,
    summary="연금복권 특정 회차",
)
async def get_pension_by_round(
    round_no: int,
    pool: asyncpg.Pool = Depends(get_pool),
):
    """회차 번호로 단일 결과. 없으면 404."""
    return or_404(await pension_service.get_pension_result_by_round(pool, round_no), "해당 회차 결과를 찾을 수 없습니다.")
