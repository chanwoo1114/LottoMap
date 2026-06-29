import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Path, Query

from app.core.database import get_pool
from app.schema.prediction_schema import RoundPredictions
from app.services import predictions_service
from app.jobs.predictions_job import get_or_create_round_predictions

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


@router.get(
    "/{round_no}",
    response_model=RoundPredictions,
    summary="회차별 AI 예측",
)
async def round_predictions(round_no: int = Path(..., ge=1)):
    """있으면 반환(미채점이면 채점), 없고 다음 회차면 생성, 과거인데 없으면 404."""
    result = await get_or_create_round_predictions(round_no)
    if result is None:
        raise HTTPException(status_code=404, detail="이 회차의 AI 예측이 없습니다.")
    return result
