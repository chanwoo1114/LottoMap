from pydantic import BaseModel, Field

class StoreQuery(BaseModel):
    sido: str | None = Field(None, description="시/도")
    sigungu: str | None = Field(None, description="시/군/구")
    address: str | None = Field(None, description="주소 키워드 (부분 일치)")
    sells_lotto: bool = Field(False, description="로또 6/45 취급점만 필터 (true면 적용)")
    sells_pension: bool = Field(False, description="연금복권 720+ 취급점만 필터 (true면 적용)")
    sells_speetto_2000: bool = Field(False, description="스피또2000 취급점만 필터 (true면 적용)")
    sells_speetto_1000: bool = Field(False, description="스피또1000 취급점만 필터 (true면 적용)")
    sells_speetto_500: bool = Field(False, description="스피또500 취급점만 필터 (true면 적용)")
    page: int = Field(1, ge=1, description="페이지 번호 (1부터)")
    size: int = Field(10, ge=1, le=10, description="페이지당 건수 (최대 10)")


class NearbyStoreQuery(BaseModel):
    lat: float = Field(..., ge=-90, le=90, description="위도 (EPSG:4326)")
    lng: float = Field(..., ge=-180, le=180, description="경도 (EPSG:4326)")
    radius_m: int = Field(1000, ge=1, le= 5000, description="반경(m), 기본 1km, 최대 5km")


class StoreResponse(BaseModel):
    id: int = Field(description="판매점 고유 ID")
    store_id: str = Field(description="동행복권 기준 판매점 고유번호")
    name: str = Field(description="판매점 상호명")
    address: str = Field(description="판매점 주소")
    phone: str = Field(description="판매점 연락처")
    sido: str | None = Field(None, description="시/도")
    sigungu: str | None = Field(None, description="시/군/구")
    dong: str | None = Field(None, description="읍/면/동")
    sells_lotto: bool = Field(description="로또 6/45 판매 여부")
    sells_pension: bool = Field(description="연금복권 720+ 판매 여부")
    sells_speetto_2000: bool = Field(description="스피또2000 판매 여부")
    sells_speetto_1000: bool = Field(description="스피또1000 판매 여부")
    sells_speetto_500: bool = Field(description="스피또500 판매 여부")
    lat: float | None = Field(None, ge=-90, le=90, description="위도 (EPSG:4326)")
    lng: float | None = Field(None, ge=-180, le=180, description="경도 (EPSG:4326)")
