from datetime import date

from pydantic import BaseModel, Field


class LottoResultsQuery(BaseModel):
    from_round: int | None = Field(None, ge=1, description="시작 회차 (포함)")
    to_round: int | None = Field(None, ge=1, description="끝 회차 (포함)")
    page: int = Field(1, ge=1, description="페이지 번호 (1부터)")
    size: int = Field(10, ge=1, le=10, description="페이지당 건수 (최대 10)")


class LottoResultResponse(BaseModel):
    round_no: int = Field(description="추첨 회차")
    draw_date: date = Field(description="추첨일")
    numbers: list[int] = Field(description="당첨번호 6개 (오름차순)")
    bonus: int = Field(description="보너스 번호")
    first_prize_amount: int = Field(description="1등 당첨금 (원)")
    first_prize_winners: int = Field(description="1등 당첨자 수")
    total_sales: int = Field(description="총 판매액 (원)")
