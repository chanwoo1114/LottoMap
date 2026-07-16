from pydantic import BaseModel, Field

class WinningStoresQuery(BaseModel):
    '''회차별 당첨 배출점 조회 요청'''
    lottery_type: str = Field(..., description="복권 종류")
    round_no: int = Field(ge=1, description="당첨 회차")
    prize_rank: int = Field(1, ge=1, le=3, description="당첨 등수")


class WinningStoreResponse(BaseModel):
    '''회차별 당첨 배출점 응답'''
    store_id: int = Field(description="판매점 고유 ID")
    name: str = Field(description="판매점 상호명")
    address: str = Field(description="판매점 주소")
    sido: str | None = Field(None, description="시도")
    sigungu: str | None = Field(None, description="시군구")
    is_online: bool = Field(False, description="인터넷 판매분 여부 (실물 점포 아님, 지도 표시 제외)")
    lat: float | None = Field(None, description="위도")
    lng: float | None = Field(None, description="경도")
    prize_rank: int = Field(description="당첨 등수")
    prize_amount: int = Field(description="당첨금")
    purchase_method: str = Field(description="구매 방식")