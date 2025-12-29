from pydantic import BaseModel, Field
from typing import Optional, List


class NHTSATestMetadata(BaseModel):
    """NHTSA 시험 메타데이터 모델"""
    test_no: int = Field(..., alias="TestNo")
    test_type: str = Field(..., alias="TestType")
    make: str = Field(..., alias="Make")
    model: str = Field(..., alias="Model")
    model_year: int = Field(..., alias="ModelYear")
    report_url: Optional[str] = None


class SignalMetadata(BaseModel):
    """가속도 센서 신호 메타데이터"""

    channel_id: int = Field(..., alias="channelId")
    sensor: str
    location: str
    url: str = Field(..., alias="downloadUrl")  # 실제 데이터 파일 경로
