from datetime import date, datetime

from pydantic import BaseModel


class PredictionItem(BaseModel):
    id: int
    model: str
    strategy: str | None = None
    numbers: list[int]
    confidence: int
    hit_count: int | None = None
    matched_bonus: bool
    created_at: datetime
    scored_at: datetime | None = None


class RoundPredictions(BaseModel):
    target_round: int
    draw_date: date | None = None
    winning_numbers: list[int] | None = None
    bonus: int | None = None
    predictions: list[PredictionItem]
