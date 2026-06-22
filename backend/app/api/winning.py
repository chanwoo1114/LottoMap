from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, Query

from app.core.database import get_pool
from app.schema.winning_schema import WinningStoresQuery, WinningStoreResponse
from app.services import winning_service

router = APIRouter(prefix="/winning-store", tags=['당첨 판매점'])

@router.get(
    "",
    response_model=list[WinningStoreResponse],
    summary="회차별 당첨 배풀점",
)
async def get_winning_store(
    q: Annotated[WinningStoresQuery, Query()],
    pool: asyncpg.Pool = Depends(get_pool),
):
    return await winning_service.get_winning_stores(
        pool, q.lottery_type, q.round_no, q.prize_rank
    )