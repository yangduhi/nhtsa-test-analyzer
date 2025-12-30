# -*- coding: utf-8 -*-
"""
API 응답을 Pydantic 모델로 변환하는 파서 모듈.

이 모듈은 원시 API 응답(JSON)을 사전에 정의된 Pydantic 모델(`NHTSARecord`)로
안전하게 변환하는 함수를 제공합니다. 데이터 유효성 검사 오류나 예기치 않은
데이터 구조에 대한 예외 처리를 포함하여 파싱 과정의 안정성을 보장합니다.

Functions:
    parse_record_to_model: 원시 dict를 NHTSARecord 모델 객체로 변환.
"""
from typing import Dict, Any, Optional
from loguru import logger
from pydantic import ValidationError
from src.core.models import NHTSARecord


def parse_record_to_model(
    test_id: int, api_data: Dict[str, Any]
) -> Optional[NHTSARecord]:
    """
    원시 API 응답 dict를 NHTSARecord Pydantic 모델로 파싱합니다.

    데이터가 없거나, 필수 필드가 누락되었거나, 유효성 검사에 실패하는 등의
    다양한 예외 상황을 처리하여, 오류 발생 시에도 전체 프로세스가 중단되지
    않도록 `None`을 반환하고 경고 로그를 남깁니다.

    Args:
        test_id (int): 현재 처리 중인 테스트의 ID (로깅 목적).
        api_data (Dict[str, Any]): NHTSA API로부터 받은 원시 JSON 응답 (dict 형태).

    Returns:
        Optional[NHTSARecord]: 파싱과 유효성 검사에 모두 성공한 경우
                               `NHTSARecord` 객체를, 실패한 경우 `None`을 반환.
    """
    results = api_data.get("results", [])
    if not results:
        logger.debug(f"Skipping Test ID {test_id}: 'results' field is empty.")
        return None

    # The actual data payload is the first element in the 'results' list.
    payload = results[0]

    # Defensively check for the existence of the core 'TEST' metadata.
    # The API sometimes returns {"TEST": null}, which should be treated as no data.
    if not payload.get("TEST"):
        logger.warning(f"Skipping Test ID {test_id}: 'TEST' metadata is null or missing.")
        return None

    try:
        # Attempt to create and validate the Pydantic model.
        return NHTSARecord(**payload)

    except ValidationError as e:
        # Log a concise summary of validation errors instead of a full stack trace.
        error_summary = "; ".join([f"{err['loc']}: {err['msg']}" for err in e.errors()])
        logger.warning(f"Validation failed for Test ID {test_id}: {error_summary}")
        return None

    except Exception as e:
        # Catch any other unexpected errors during parsing.
        logger.error(f"An unexpected parsing error occurred for Test ID {test_id}: {e}")
        return None