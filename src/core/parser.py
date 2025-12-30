from typing import Dict, Any, Optional
from loguru import logger
from pydantic import ValidationError
from src.core.models import NHTSARecord


def parse_record_to_model(
    test_id: int, api_data: Dict[str, Any]
) -> Optional[NHTSARecord]:
    """
    API Raw Response -> Pydantic Model 변환.

    데이터가 불완전하거나 비어있는 경우(TEST=null)를 방어하여
    불필요한 에러 트레이스백 출력을 방지합니다.
    """
    results = api_data.get("results", [])

    # 1. 결과 리스트가 비어있는 경우
    if not results:
        return None

    payload = results[0]

    # 2. [수정] 'TEST' 필드가 아예 없거나 값이 None인 경우 체크
    # API가 {"TEST": null, ...} 형태로 응답하는 케이스 방어
    test_data = payload.get("TEST")
    if test_data is None:
        # 에러 레벨이 아닌 경고 레벨로 로그를 남기고 스킵 처리
        logger.warning(f"Skipping Test ID {test_id}: 'TEST' metadata is empty (Null).")
        return None

    try:
        # 3. Pydantic 모델 변환
        return NHTSARecord(**payload)

    except ValidationError as e:
        # Validation 에러 발생 시 전체 스택트레이스 대신 요약 정보만 출력
        error_msgs = "; ".join([f"{err['loc']}: {err['msg']}" for err in e.errors()])
        logger.warning(f"Validation failed for Test ID {test_id}: {error_msgs}")
        return None

    except Exception as e:
        logger.error(f"Unexpected parsing error for Test ID {test_id}: {e}")
        return None
