from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Annotated
import asyncpg

from app.core.database import get_pool
from app.services import stores_service
from app.schema.store_schema import (
    StoreQuery, StoreResponse,
    NearbyStoreQuery, WinningStatsResponse,
)

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
    summary="화면 영역(bbox) 내 판매점 조회",
)
async def nearby_stores(
    q: Annotated[NearbyStoreQuery, Query()],
    pool: asyncpg.Pool = Depends(get_pool),
):
    """남서·북동 좌표로 정의된 사각형 영역 내 판매점 반환"""
    return await stores_service.get_nearby_stores(
        pool,
        q.min_lat,
        q.min_lng,
        q.max_lat,
        q.max_lng,
        q.limit,
    )

@router.get(
    "/{store_id}/winning-stats",
    response_model=WinningStatsResponse,
    summary="판매점 당첨 배출 통계",
)
async def store_winning_stats(
    store_id: int,
    pool: asyncpg.Pool = Depends(get_pool),
):
    """판매점의 로또/연금/스피또 당첨 배출 횟수 집계 반환"""
    return await stores_service.get_winning_stats(pool, store_id)


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