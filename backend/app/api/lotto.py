import logging
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.database import get_pool
from app.schema.lotto_schema import LottoResultResponse, LottoResultsQuery
from app.services import lotto_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/lotto", tags=["로또"])


@router.get(
    "/results",
    response_model=list[LottoResultResponse],
    summary="로또 회차 목록",
)
async def list_lotto_results(
    q: Annotated[LottoResultsQuery, Query()],
    pool: asyncpg.Pool = Depends(get_pool),
):
    """회차 범위(`from_round`, `to_round`)로 필터링. 최신 회차 우선."""
    return await lotto_service.search_lotto_results(pool, q)


@router.get(
    "/results/latest",
    response_model=LottoResultResponse,
    summary="로또 최신 회차",
)
async def get_latest_lotto(
    pool: asyncpg.Pool = Depends(get_pool),
):
    """가장 최근 회차 한 건. 데이터 없으면 404."""
    result = await lotto_service.get_latest_lotto_result(pool)
    if not result:
        raise HTTPException(404, "로또 결과가 없습니다.")
    return result


@router.get(
    "/results/{round_no}",
    response_model=LottoResultResponse,
    summary="로또 특정 회차",
)
async def get_lotto_by_round(
    round_no: int,
    pool: asyncpg.Pool = Depends(get_pool),
):
    """회차 번호로 단일 결과. 없으면 404."""
    result = await lotto_service.get_lotto_result_by_round(pool, round_no)
    if not result:
        raise HTTPException(404, "해당 회차 결과를 찾을 수 없습니다.")
    return result
