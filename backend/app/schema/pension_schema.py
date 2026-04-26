from datetime import date

from pydantic import BaseModel, Field


class PensionResultsQuery(BaseModel):
    from_round: int | None = Field(None, ge=1, description="시작 회차 (포함)")
    to_round: int | None = Field(None, ge=1, description="끝 회차 (포함)")
    page: int = Field(1, ge=1, description="페이지 번호 (1부터)")
    size: int = Field(10, ge=1, le=10, description="페이지당 건수 (최대 10)")


class PensionResultResponse(BaseModel):
    round_no: int = Field(description="추첨 회차")
    draw_date: date = Field(description="추첨일")
    first_prize_group: int = Field(description="1등 당첨번호의 조 (1~5)")
    first_prize_number: str = Field(description="1등 당첨번호 6자리")
    bonus_number: str = Field(description="보너스 당첨번호 6자리")
