import asyncpg
from fastapi import APIRouter, Depends

from app.core.database import get_pool
from app.schema.speetto_schema import SpeettoGameResponse
from app.services import speetto_service

router = APIRouter(prefix="/speetto", tags=["스피또"])


@router.get(
    "/games",
    response_model=list[SpeettoGameResponse],
    summary="스피또 판매 중 게임 현황",
)
async def list_speetto_games(
    pool: asyncpg.Pool = Depends(get_pool),
):
    """판매기한 내 게임의 등수별 잔여수량. 판매종료 회차는 제외."""
    return await speetto_service.get_onsale_games(pool)
