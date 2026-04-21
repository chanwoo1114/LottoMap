from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Annotated
import asyncpg
import logging

from app.core.database import get_pool
from app.services import stores_service
from app.schema.store_schema import (
    StoreQuery, StoreResponse,
    NearbyStoreQuery,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/store", tags=["판매점"])

@router.get(
    "",
    response_model=list[StoreResponse],
    summary="판매점 검색",
)
async def list_stores(
    q: Annotated[StoreQuery, Query()],
    pool: asyncpg.Pool = Depends(get_pool),
):
    """시/도, 시/군/구, 주소 키워드, 취급 복권 종류로 필터링 (페이지당 최대 10건)"""
    return await stores_service.search_stores(pool, q)


@router.get(
    "/nearby",
    response_model=list[StoreResponse],
    summary="반경 내 판매점 조회",
)
async def nearby_stores(
    q: Annotated[NearbyStoreQuery, Query()],
    pool: asyncpg.Pool = Depends(get_pool),
):
    """위도·경도 기준 반경(m, 최대 5km) 내 판매점을 가까운 순으로 반환"""
    return await stores_service.get_nearby_stores(
        pool,
        q.lat,
        q.lon,
        q.radius_m,
    )

@router.get(
    "/{store_id}",
    response_model=StoreResponse,
    summary="판매점 상세 조회",
)
async def get_store(
    store_id: int,
    pool: asyncpg.Pool = Depends(get_pool),
):
    """고유 ID로 단일 판매점 정보 반환. 없으면 404"""
    result = await stores_service.get_store_by_id(pool, store_id)
    if not result:
        raise HTTPException(404, "판매점을 찾을 수 없습니다.")
    return result