from typing import Literal

from pydantic import BaseModel, Field


StatStrategy = Literal[
    "hot", "cold", "balanced", "overdue",
    "pattern_match", "contrarian", "streak_based",
]


class StatisticalQuery(BaseModel):
    strategy: StatStrategy = Field("balanced", description="번호 생성 전략")
    count: int = Field(5, ge=1, le=10, description="생성할 세트 수 (1~10)")
    exclude: list[int] | None = Field(None, description="제외할 번호 목록")
    include: list[int] | None = Field(None, description="반드시 포함할 번호 목록")


class AIQuery(BaseModel):
    count: int = Field(5, ge=1, le=10, description="생성할 세트 수 (1~10)")
    temperature: float = Field(1.5, gt=0, le=3.0, description="샘플링 온도 (낮을수록 결정적)")
    exclude: list[int] | None = Field(None, description="제외할 번호 목록")
    include: list[int] | None = Field(None, description="반드시 포함할 번호 목록")


PensionStrategy = Literal["hot", "cold", "balanced", "random"]


class PensionQuery(BaseModel):
    strategy: PensionStrategy = Field("balanced", description="번호 생성 전략")
    count: int = Field(5, ge=1, le=10, description="생성할 세트 수 (1~10)")
    fixed_group: int | None = Field(None, ge=1, le=5, description="조 고정 (1~5), 미지정 시 통계 기반 선택")


class GeneratorResult(BaseModel):
    """각 세트는 생성기별로 구조가 다르므로 dict로 유연하게 반환"""
    numbers: list[int]
    sum: int
    ac_value: int
    odd_even: str
    consecutive_pairs: int

    class Config:
        extra = "allow"


class GeneratorResponse(BaseModel):
    results: list[dict]
    count: int