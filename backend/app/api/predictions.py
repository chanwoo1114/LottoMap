import asyncpg
from fastapi import APIRouter, Depends, Query

from app.core.database import get_pool
from app.schema.prediction_schema import RoundPredictions
from app.services import predictions_service

router = APIRouter(prefix="/predictions", tags=["AI 예측"])


@router.get(
    "/recent",
    response_model=list[RoundPredictions],
    summary="최근 회차 AI/통계 예측 + 실제 당첨번호 비교",
)
async def recent_predictions(
    rounds: int = Query(10, ge=1, le=50, description="조회할 회차 수 (1~50)"),
    pool: asyncpg.Pool = Depends(get_pool),
):
    """target_round DESC 순 N개 회차의 모든 예측을 모델/전략별로 반환.
    추첨이 끝난 회차는 winning_numbers·hit_count·matched_bonus 까지 채워짐."""
    return await predictions_service.list_recent_predictions(pool, rounds)
