from datetime import date, datetime

from pydantic import BaseModel, Field, field_validator


class SavedNumberCreate(BaseModel):
    """저장할 로또 번호 요청"""
    numbers: list[int] = Field(..., description="로또 번호 6개 (1~45, 중복 불가)")
    source: str = Field("generated", max_length=20, description="입력 출처 (manual/generated/qr)")
    memo: str = Field("", max_length=100, description="메모")

    @field_validator("numbers")
    @classmethod
    def _validate_numbers(cls, v: list[int]) -> list[int]:
        if len(v) != 6:
            raise ValueError("번호는 6개여야 합니다.")
        if len(set(v)) != 6:
            raise ValueError("번호가 중복될 수 없습니다.")
        if any(n < 1 or n > 45 for n in v):
            raise ValueError("번호는 1~45 사이여야 합니다.")
        return sorted(v)


class SavedNumberResponse(BaseModel):
    """저장한 번호 + 당첨 대조 결과"""
    id: int = Field(description="저장 항목 고유 ID")
    round_no: int = Field(description="대상 회차 (저장 시점의 다음 추첨 회차)")
    numbers: list[int] = Field(description="선택 번호 6개")
    source: str = Field(description="입력 출처")
    matched_count: int | None = Field(None, description="일치 개수 (미채점 시 null)")
    matched_bonus: bool | None = Field(None, description="보너스 일치 여부")
    matched_rank: int | None = Field(None, description="0=낙첨, 1~5=당첨 등수, null=미채점")
    memo: str = Field(description="메모")
    created_at: datetime = Field(description="저장 시각")
    draw_date: date | None = Field(None, description="해당 회차 추첨일 (미추첨 시 null)")
    winning_numbers: list[int] | None = Field(None, description="해당 회차 당첨번호 6개 (미추첨 시 null)")
    winning_bonus: int | None = Field(None, description="해당 회차 보너스 번호 (미추첨 시 null)")
