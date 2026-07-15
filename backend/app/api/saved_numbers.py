import asyncpg
from fastapi import APIRouter, Depends, status

from app.api.deps import get_current_user_id
from app.core.database import get_pool
from app.schema.saved_numbers_schema import SavedNumberCreate, SavedNumberResponse
from app.services import saved_numbers_service

router = APIRouter(prefix="/saved-numbers", tags=["저장한 번호"])


@router.get(
    "",
    response_model=list[SavedNumberResponse],
    summary="내 저장 번호 목록",
)
async def list_saved_numbers(
    user_id: int = Depends(get_current_user_id),
    pool: asyncpg.pool.Pool = Depends(get_pool),
):
    return await saved_numbers_service.list_my_numbers(pool, user_id)


@router.post(
    "",
    response_model=SavedNumberResponse,
    status_code=status.HTTP_201_CREATED,
    summary="번호 저장",
)
async def create_saved_number(
    body: SavedNumberCreate,
    user_id: int = Depends(get_current_user_id),
    pool: asyncpg.pool.Pool = Depends(get_pool),
):
    return await saved_numbers_service.add_number(
        pool, user_id, body.numbers, body.source, body.memo
    )


@router.delete(
    "/{number_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="저장 번호 삭제",
)
async def delete_saved_number(
    number_id: int,
    user_id: int = Depends(get_current_user_id),
    pool: asyncpg.pool.Pool = Depends(get_pool),
):
    await saved_numbers_service.remove_number(pool, user_id, number_id)
