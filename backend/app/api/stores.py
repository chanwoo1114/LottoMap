from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Annotated
import asyncpg
import logging

from app.core.database import get_pool
from app.services import stores_service
from app.schema.store_schema import (
    NearbyStoreQuery,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/store", tags=["판매점"])

@router.get("/{store_id}")
async def get_store(
    store_id: int,
    pool: asyncpg.Pool = Depends(get_pool),
):
    '''판매점 상세 조회'''
    result = await stores_service.get_store_by_id(pool, store_id)
    if not result
        raise HTTPException(404, "판매점을 찾을 수 없습니다.")
    return result

@router.get("/nearby/")
async def nearby_stores(
    q: Annotated[NearbyStoreQuery, Query()],
):
    '''반경 내 판매점 조회'''

