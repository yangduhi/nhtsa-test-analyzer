# -*- coding: utf-8 -*-
"""
NHTSA API 응답을 위한 Pydantic 데이터 모델 정의 모듈.

이 모듈은 NHTSA의 복잡한 JSON 응답 구조를 체계적이고 타입-안전(type-safe)한
Python 객체로 변환하기 위한 Pydantic 모델들을 정의합니다. 각 클래스는 API 응답의
주요 섹션(예: TEST, VEHICLE)에 해당하며, 필드 유효성 검사 및 데이터 전처리를
포함하여 데이터의 일관성과 정확성을 보장합니다.

Classes:
    TestInfo: 테스트 기본 정보 모델.
    Vehicle: 차량 제원, 중량 및 파손 정보 모델.
    Occupant: 탑승자 정보 및 상해 결과 모델.
    ResourceUrl: 데이터 파일 URL 모델.
    Report: 리포트 파일 정보 모델.
    NHTSARecord: API 응답 전체를 대표하는 최상위 모델.
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator, model_validator


class TestInfo(BaseModel):
    """테스트 기본 정보 (API 'TEST' 섹션)."""
    test_id: int = Field(alias="TSTNO", description="테스트 고유 ID")
    test_date: Optional[str] = Field(None, alias="TSTDAT", description="테스트 수행일")
    title: Optional[str] = Field(None, alias="TITLE", description="테스트 제목")
    closing_speed: Optional[float] = Field(None, alias="CLSSPD", description="충돌 시 상대 속도")
    test_type: Optional[str] = Field(None, alias="TSTTYPD", description="테스트 유형")
    condition: Optional[str] = Field(None, alias="TKCOND", description="테스트 조건")
    reference: Optional[str] = Field(None, alias="TSTREF", description="참조 문서")
    crash_config: Optional[str] = Field(None, alias="TSTCFND", description="충돌 구성")


class Vehicle(BaseModel):
    """
    차량 제원, 중량 및 파손 정보 (API 'VEHICLE' 섹션).
    
    충돌 전/후 계측점(AX, BX)과 같은 복잡한 필드를 전처리하는 로직을 포함합니다.
    """
    vehicle_id: int = Field(alias="VEHNO", description="차량 번호 (테스트 내)")
    make: Optional[str] = Field(None, alias="MAKED", description="제조사")
    model: Optional[str] = Field(None, alias="MODELD", description="모델명")
    year: Optional[int] = Field(None, alias="YEAR", description="연식")
    body_type: Optional[str] = Field(None, alias="BODYD", description="차체 유형")
    vin: Optional[str] = Field(None, alias="VIN", description="차대번호")
    
    # --- Vehicle Specifications & Weight ---
    weight: Optional[float] = Field(None, alias="VEHTWT", description="시험 중량 (Test Weight)")
    wheelbase: Optional[float] = Field(None, alias="WHLBAS", description="휠베이스")
    length: Optional[float] = Field(None, alias="VEHLEN", description="전장")
    width: Optional[float] = Field(None, alias="VEHWID", description="전폭")
    
    # --- Damage Analysis ---
    vdi: Optional[str] = Field(None, alias="VDI", description="차량 파손 지수 (CDC)")
    pdof: Optional[float] = Field(None, alias="PDOF", description="주 충돌 방향")
    
    # --- Crush Profile (DPD1-6) ---
    dpd1: Optional[float] = Field(None, alias="DPD1", description="파손 깊이 1")
    dpd2: Optional[float] = Field(None, alias="DPD2", description="파손 깊이 2")
    dpd3: Optional[float] = Field(None, alias="DPD3", description="파손 깊이 3")
    dpd4: Optional[float] = Field(None, alias="DPD4", description="파손 깊이 4")
    dpd5: Optional[float] = Field(None, alias="DPD5", description="파손 깊이 5")
    dpd6: Optional[float] = Field(None, alias="DPD6", description="파손 깊이 6")

    # --- Measurement Points (Pre-grouped) ---
    pre_impact_points: Dict[str, Optional[float]] = Field(default_factory=dict, description="충돌 전 계측점 (BX1-30)")
    post_impact_points: Dict[str, Optional[float]] = Field(default_factory=dict, description="충돌 후 계측점 (AX1-30)")

    @model_validator(mode="before")
    @classmethod
    def group_measurement_points(cls, data: Any) -> Any:
        """
        'BX1', 'AX1' 등 개별 필드를 딕셔너리로 그룹화하는 전처리기.
        
        Pydantic 모델이 생성되기 전에 실행되어, 흩어져 있는 계측점 데이터를
        구조화된 딕셔너리 필드로 변환합니다.
        
        Args:
            data (Any): 모델로 전달된 원시 데이터.
            
        Returns:
            Any: 전처리된 데이터.
        """
        if not isinstance(data, dict):
            return data
            
        pre_points, post_points = {}, {}
        for i in range(1, 31):
            if (val := data.get(f"BX{i}")) is not None:
                pre_points[f"BX{i}"] = val
            if (val := data.get(f"AX{i}")) is not None:
                post_points[f"AX{i}"] = val
                
        data["pre_impact_points"] = pre_points
        data["post_impact_points"] = post_points
        return data


class Occupant(BaseModel):
    """탑승자 정보 및 상해 결과 (API 'OCCUPANT' 섹션)."""
    seat_pos: Optional[str] = Field(None, alias="SEPOSN", description="좌석 위치 (예: Driver)")
    type: Optional[str] = Field(None, alias="OCCTYPD", description="더미 타입 (예: Hybrid III)")
    age: Optional[int] = Field(None, alias="OCCAGE", description="더미 연령")
    sex: Optional[str] = Field(None, alias="OCCSEXD", description="더미 성별")

    # --- Key Injury Criteria ---
    hic: Optional[float] = Field(None, alias="HIC", description="두부 상해 기준 (Head Injury Criterion)")
    chest_deflection: Optional[float] = Field(None, alias="CD", description="흉부 변위량 (mm)")
    femur_left: Optional[float] = Field(None, alias="LFEM", description="좌측 대퇴골 하중")
    femur_right: Optional[float] = Field(None, alias="RFEM", description="우측 대퇴골 하중")
    neck_tension: Optional[float] = Field(None, alias="TNT", description="목 인장 하중")
    chest_g: Optional[float] = Field(None, alias="CG", description="흉부 가속도")


class ResourceUrl(BaseModel):
    """주요 데이터 파일 URL (API 'URL' 섹션)."""
    url_tdms: Optional[str] = Field(None, alias="URL_TDMS", description="TDMS 데이터 파일 URL")
    url_uds: Optional[str] = Field(None, alias="URL_UDS", description="UDS 데이터 파일 URL")
    url_zip: Optional[str] = Field(None, alias="URL_EV5", description="ZIP 아카이브 URL")


class Report(BaseModel):
    """관련 리포트 파일 정보 (API 'REPORTS' 섹션)."""
    filename: Optional[str] = Field(None, alias="ORIG_FILENAME", description="원본 파일명")
    url: Optional[str] = Field(None, alias="URL", description="리포트 URL")
    filesize: Optional[str] = Field(None, alias="FILESIZE", description="파일 크기")


class NHTSARecord(BaseModel):
    """
    NHTSA API 응답 전체를 대표하는 최상위 모델.
    
    API 응답의 각 섹션을 하위 모델로 포함하며, 리스트 형태의 데이터가
    `None`으로 반환될 경우 빈 리스트로 초기화하는 유효성 검사를 수행합니다.
    """
    test_info: TestInfo = Field(alias="TEST")
    vehicles: List[Vehicle] = Field(default_factory=list, alias="VEHICLE")
    occupants: List[Occupant] = Field(default_factory=list, alias="OCCUPANT")
    urls: Optional[ResourceUrl] = Field(None, alias="URL")
    reports: List[Report] = Field(default_factory=list, alias="REPORTS")

    @field_validator("vehicles", "occupants", "reports", mode="before")
    @classmethod
    def ensure_list(cls, v: Any) -> List[Any]:
        """
        리스트 필드가 `None`일 경우 빈 리스트를 반환하도록 보장하는 유효성 검사기.
        
        API가 데이터가 없을 때 `null`을 반환하는 경우에 대한 방어 로직입니다.
        
        Args:
            v (Any): 검증할 필드의 원시 값.
        
        Returns:
            List[Any]: `None`이 아닌 경우 원래 값을, `None`인 경우 빈 리스트를 반환.
        """
        return v if v is not None else []