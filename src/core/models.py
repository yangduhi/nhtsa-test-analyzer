from pydantic import BaseModel, Field, ConfigDict, model_validator
from typing import Optional, Any, Union


class NHTSATestMetadata(BaseModel):
    """NHTSA 시험 메타데이터 모델"""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    test_no: int = Field(..., alias="testNo")
    test_type: Optional[str] = Field(None, alias="testType")

    # 기본값 설정 (데이터가 없으면 Unknown 처리)
    make: Optional[str] = Field("Unknown", alias="make")
    model: Optional[str] = Field("Unknown", alias="model")
    model_year: Optional[int] = Field(None, alias="modelYear")

    # [수정 핵심] API가 상세 정보 대신 URL 문자열만 줄 때를 대비해 str 타입 허용
    vehicle_info: Optional[Union[dict, str]] = Field(None, alias="vehicleInformation")

    @model_validator(mode="before")
    @classmethod
    def flatten_nested_data(cls, data: Any) -> Any:
        """vehicleInformation 내부의 데이터를 최상위로 끌어올림"""
        if isinstance(data, dict):
            v = data.get("vehicleInformation")

            # [수정 핵심] v가 딕셔너리일 때만 내부 데이터를 추출
            # 문자열(URL)이면 추출할 수 없으므로 건너뜀
            if isinstance(v, dict):
                data["make"] = data.get("make") or v.get("make")
                data["model"] = data.get("model") or v.get("model")
                # API 필드명이 다를 수 있으므로 여러 후보군 체크
                data["modelYear"] = (
                    data.get("modelYear")
                    or v.get("modelYear")
                    or v.get("vehicleModelYear")
                )
        return data


class SignalMetadata(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    channel_id: int = Field(..., alias="channelId")
    sensor: str = Field(..., alias="sensor")
    location: str = Field(..., alias="location")
    url: str = Field(..., alias="downloadUrl")
