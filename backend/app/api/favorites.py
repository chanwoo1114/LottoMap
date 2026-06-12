import asyncpg
from fastapi import APIRouter, Depends, status

from app.api.deps import get_current_user_id
from app.core.database import get_pool
from app.schema.store_schema import StoreResponse
from app.services import favorites_service

router = APIRouter(prefix="/favorites", tags=["즐겨찾기 목록"])

@router.get(
    "",
    response_model=list[StoreResponse],
    summary="내 즐겨찾기 목록"
)
async def get_my_favorites_list(
    user_id: int = Depends(get_current_user_id),
    pool: asyncpg.pool.Pool = Depends(get_pool)
):
    return await favorites_service.get_my_favorites_list(pool, user_id)

@router.post(
    "/{store_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="즐겨찾기 추가"
)
async def add_favorite(
    store_id: int,
    user_id: int = Depends(get_current_user_id),
    pool: asyncpg.pool.Pool = Depends(get_pool)
):
    await favorites_service.add_favorite(pool, user_id, store_id)

@router.delete(
    "/{store_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="즐겨찾기 제거"
)
async def remove_favorite(
    store_id: int,
    user_id: int = Depends(get_current_user_id),
    pool: asyncpg.pool.Pool = Depends(get_pool)
):
    await favorites_service.remove_favorite(pool, user_id, store_id)