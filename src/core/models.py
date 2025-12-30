"""
Pydantic models for NHTSA Test Database API response.
FINAL VERSION: Includes Vehicle Specs, Test Weight, and Occupant Injury Criteria.
"""

from typing import List, Optional, Dict
from pydantic import BaseModel, Field, field_validator, model_validator


class TestInfo(BaseModel):
    """TEST 섹션: 테스트 기본 정보"""

    test_id: int = Field(alias="TSTNO")
    test_date: Optional[str] = Field(None, alias="TSTDAT")
    title: Optional[str] = Field(None, alias="TITLE")
    closing_speed: Optional[float] = Field(None, alias="CLSSPD")
    test_type: Optional[str] = Field(None, alias="TSTTYPD")
    condition: Optional[str] = Field(None, alias="TKCOND")
    reference: Optional[str] = Field(None, alias="TSTREF")
    crash_config: Optional[str] = Field(None, alias="TSTCFND")


class Vehicle(BaseModel):
    """
    VEHICLE 섹션: 차량 제원, 중량, 파손 정보
    """

    vehicle_id: int = Field(alias="VEHNO")
    make: Optional[str] = Field(None, alias="MAKED")
    model: Optional[str] = Field(None, alias="MODELD")
    year: Optional[int] = Field(None, alias="YEAR")
    body_type: Optional[str] = Field(None, alias="BODYD")
    vin: Optional[str] = Field(None, alias="VIN")

    # [추가] 차량 제원 및 중량 (Specifications & Weight)
    weight: Optional[float] = Field(None, alias="VEHTWT")  # 시험 중량 (Test Weight)
    wheelbase: Optional[float] = Field(None, alias="WHLBAS")  # 휠베이스
    length: Optional[float] = Field(None, alias="VEHLEN")  # 전장
    width: Optional[float] = Field(None, alias="VEHWID")  # 전폭

    # [추가] 충돌 분석 데이터 (Damage Analysis)
    vdi: Optional[str] = Field(None, alias="VDI")  # 파손 지수 (CDC)
    pdof: Optional[float] = Field(None, alias="PDOF")  # 주충돌 방향

    # [추가] 파손 깊이 (Crush Profile, DPD1~6)
    dpd1: Optional[float] = Field(None, alias="DPD1")
    dpd2: Optional[float] = Field(None, alias="DPD2")
    dpd3: Optional[float] = Field(None, alias="DPD3")
    dpd4: Optional[float] = Field(None, alias="DPD4")
    dpd5: Optional[float] = Field(None, alias="DPD5")
    dpd6: Optional[float] = Field(None, alias="DPD6")

    # 계측점 데이터 (BX: Before, AX: After) - 별도 처리
    pre_impact_points: Dict[str, Optional[float]] = Field(default_factory=dict)
    post_impact_points: Dict[str, Optional[float]] = Field(default_factory=dict)

    @model_validator(mode="before")
    def extract_dimensions(cls, data):
        """AX1~30, BX1~30 필드를 딕셔너리로 그룹화"""
        if not isinstance(data, dict):
            return data
        pre, post = {}, {}
        for i in range(1, 31):
            if val := data.get(f"BX{i}"):
                pre[f"BX{i}"] = val
            if val := data.get(f"AX{i}"):
                post[f"AX{i}"] = val
        data["pre_impact_points"] = pre
        data["post_impact_points"] = post
        return data


class Occupant(BaseModel):
    """
    OCCUPANT 섹션: 탑승자 정보 및 상해 결과 (Injury Criteria)
    """

    seat_pos: Optional[str] = Field(
        None, alias="SEPOSN"
    )  # 좌석 위치 (Driver, Passenger)
    type: Optional[str] = Field(None, alias="OCCTYPD")  # 더미 타입 (Hybrid III 등)
    age: Optional[int] = Field(None, alias="OCCAGE")
    sex: Optional[str] = Field(None, alias="OCCSEXD")

    # [추가] 주요 상해 지표
    hic: Optional[float] = Field(None, alias="HIC")  # 두부 상해 기준값
    chest_deflection: Optional[float] = Field(None, alias="CD")  # 흉부 압박량 (mm)
    femur_left: Optional[float] = Field(None, alias="LFEM")  # 좌측 대퇴부 하중
    femur_right: Optional[float] = Field(None, alias="RFEM")  # 우측 대퇴부 하중
    neck_tension: Optional[float] = Field(None, alias="TNT")  # (있다면) 목 인장 하중
    chest_g: Optional[float] = Field(None, alias="CG")  # (있다면) 흉부 가속도


class ResourceUrl(BaseModel):
    """URL 섹션"""

    url_tdms: Optional[str] = Field(None, alias="URL_TDMS")
    url_uds: Optional[str] = Field(None, alias="URL_UDS")
    url_zip: Optional[str] = Field(None, alias="URL_EV5")


class Report(BaseModel):
    """REPORTS 섹션"""

    filename: Optional[str] = Field(None, alias="ORIG_FILENAME")
    url: Optional[str] = Field(None, alias="URL")
    filesize: Optional[str] = Field(None, alias="FILESIZE")


class NHTSARecord(BaseModel):
    """최상위 레코드 모델"""

    test_info: TestInfo = Field(alias="TEST")
    vehicles: List[Vehicle] = Field(default_factory=list, alias="VEHICLE")
    occupants: List[Occupant] = Field(default_factory=list, alias="OCCUPANT")
    urls: Optional[ResourceUrl] = Field(None, alias="URL")
    reports: List[Report] = Field(default_factory=list, alias="REPORTS")

    @field_validator("vehicles", "occupants", "reports", mode="before")
    def check_none_list(cls, v):
        return v if v is not None else []
