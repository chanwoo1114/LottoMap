from datetime import date, datetime

from pydantic import BaseModel, Field


class SpeettoGameResponse(BaseModel):
    """스피또 판매 중 게임의 잔여수량 현황 응답"""
    game_id: str = Field(description="종류+회차 조합 키 (예: st2000_68)")
    name: str = Field(description="게임명 (예: 스피또2000)")
    game_type: str = Field(description="스피또 종류 (st2000, st1000, st500)")
    round_no: int = Field(description="회차 번호 (종류별 독립 채번)")
    price: int = Field(description="1장 가격 (원)")
    sale_end_date: date | None = Field(description="판매기한")
    prize_claim_end_date: date | None = Field(description="당첨금 지급기한")
    image_url: str | None = Field(description="복권 이미지 전체 URL")
    total_first_prizes: int = Field(description="1등 총 발행 매수")
    remaining_first_prizes: int = Field(description="1등 잔여 매수")
    total_second_prizes: int = Field(description="2등 총 발행 매수")
    remaining_second_prizes: int = Field(description="2등 잔여 매수")
    total_third_prizes: int = Field(description="3등 총 발행 매수")
    remaining_third_prizes: int = Field(description="3등 잔여 매수")
    intake_rate: int = Field(description="전국 판매점 입고율 (%)")
    updated_at: datetime = Field(description="마지막 수집 일시")
