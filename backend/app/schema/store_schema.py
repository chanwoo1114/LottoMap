from pydantic import BaseModel, Field



class StoreResponse(BaseModel):
    id: int = Field(description="판매점 고유 ID")
    store_id: str = Field(description="동행복권 기준 판매점 고유번호")
    name: str = Field(description="판매점 상호명")
    address: str = Field(description="판매점 주소")
    phone: str = Field(description="판매점 연락처")
    sido: str = Field(description="시/도")
    sigungu: str = Field(description="시/군/구")
    dong: str = Field(description="읍/면/동")
    sells_lotto: bool = Field(description="로또 6/45 판매 여부")
    sells_pension: bool = Field(description="연금복권 720+ 판매 여부")
    sells_speetto_2000: bool = Field(description="스피또2000 판매 여부")
    sells_speetto_1000: bool = Field(description="스피또1000 판매 여부")
    sells_speetto_500: bool = Field(description="스피또500 판매 여부")
    lat: float | None = Field(None, ge=-90, le=90, description="위도 (EPSG:4326)")
    lon: float | None = Field(None, ge=-180, le=180, description="경도 (EPSG:4326)")


class NearbyStoreQuery(BaseModel):
    lat: float = Field(..., ge=-90, le=90, description="위도 (EPSG:4326)")
    lon: float = Field(..., ge=-180, le=180, description="경도 (EPSG:4326)")
    radius_m: int = Field(1000, ge=1, le= 50000, description="반경(m), 기본 1km, 최대 5km")


