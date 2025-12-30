"""
Pulse Analysis & Data Loading Module.
Updated: Adds fallback logic to parse ISO-MME style channel names (e.g., 10VEHCCG0000AC1P)
when explicit metadata is missing.
"""

import numpy as np
from nptdms import TdmsFile
from typing import Optional, Tuple, Dict, Any
from loguru import logger


class CrashPulseAnalyzer:
    def __init__(self, tdms_path: str):
        self.tdms_path = tdms_path
        self.tdms_file = None
        try:
            self.tdms_file = TdmsFile.read(tdms_path)
        except Exception as e:
            logger.error(f"Failed to read TDMS file: {tdms_path} -> {e}")

    def find_vehicle_accel_channel(self) -> Optional[Any]:
        """
        차체 거동 분석에 적합한 X축 가속도 센서를 찾습니다.
        1단계: 메타데이터(SENLOCD, AXISD) 확인
        2단계: 메타데이터가 없으면 채널 이름(ISO 코드) 파싱
        """
        if not self.tdms_file:
            return None

        # [우선순위 1] 리어 실 / 플로어 (가장 노이즈 적음)
        # [우선순위 2] 무게중심 (CG)
        # [우선순위 3] B필러

        candidates = []

        for group in self.tdms_file.groups():
            for channel in group.channels():
                # 1. 메타데이터 읽기
                props = channel.properties
                sen_type = str(props.get("SENTYPD", "")).upper()
                sen_loc = str(props.get("SENLOCD", "")).upper()
                axis = str(props.get("AXISD", "")).upper()

                # 채널 이름 분석 (메타데이터 없을 때 대비)
                name = channel.name.upper()

                # --- 판별 로직 ---
                is_x_accel = False
                location_score = 0  # 높을수록 우선순위 높음
                found_loc = "Unknown"

                # Case A: 메타데이터가 살아있는 경우
                if "ACCEL" in sen_type and ("X" in axis or "LONG" in axis):
                    is_x_accel = True
                    # 위치 점수 매기기
                    if "REAR" in sen_loc and ("SILL" in sen_loc or "FLOOR" in sen_loc):
                        location_score = 3
                        found_loc = sen_loc
                    elif "CG" in sen_loc:
                        location_score = 2
                        found_loc = sen_loc
                    elif "PILLAR" in sen_loc:
                        location_score = 1
                        found_loc = sen_loc

                # Case B: 메타데이터가 죽어있고('N/A'), 이름으로 판별해야 하는 경우
                # 이름 패턴 예: 10VEHCCG0000AC1P (10:차량, VEHCCG:위치, AC:가속도, 1:X축)
                elif "AC1" in name or "ACX" in name:  # AC1 = X축 가속도
                    is_x_accel = True

                    # 위치 코드 해석
                    if (
                        "LERE" in name or "RIRE" in name
                    ):  # LEft REar / RIght REar (Sill/Floor 추정)
                        location_score = 3
                        found_loc = "Rear Sill/Floor (From Name)"
                    elif "CG" in name:  # VEHCCG
                        location_score = 2
                        found_loc = "Vehicle CG (From Name)"
                    elif "PILLAR" in name or "PIL" in name:
                        location_score = 1
                        found_loc = "B-Pillar (From Name)"

                # 후보 등록
                if is_x_accel and location_score > 0:
                    candidates.append(
                        {"channel": channel, "score": location_score, "loc": found_loc}
                    )

        # 점수가 가장 높은 후보 선택
        if candidates:
            # 점수(score) 내림차순 정렬
            candidates.sort(key=lambda x: x["score"], reverse=True)
            best = candidates[0]
            # logger.info(f"Selected Sensor: {best['loc']} ({best['channel'].name})")
            return best["channel"]

        return None

    def preprocess_signal(
        self, time_s: np.ndarray, raw_g: np.ndarray
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        신호 전처리 (0점 조정, 극성 보정, T0 정렬)
        """
        # 1. 0점 조정 (Bias Removal)
        # 데이터가 충분하면 앞부분 50개 샘플 사용 (노이즈 안정화 고려)
        n_samples = min(len(raw_g), 50)
        bias = np.mean(raw_g[:n_samples])
        zeroed_g = raw_g - bias

        # 2. 극성 보정 (Polarity Check)
        # 정면 충돌(감속)은 물리적으로 속도가 줄어들어야 함.
        # 절대값 최대 피크가 양수(+)라면 센서가 반대로 달린 것이므로 반전.
        # (단, Rebound로 인한 양수 피크가 더 클 수도 있으나, 초기 100ms 내 피크를 보는 게 정확함)
        # 여기서는 전체 피크 기준으로 단순화
        max_idx = np.argmax(np.abs(zeroed_g))
        if zeroed_g[max_idx] > 0:
            zeroed_g = zeroed_g * -1

        # 3. T0 정렬 (Time Zero Alignment)
        # 0.5G 트리거
        threshold_g = 0.5
        trigger_indices = np.where(np.abs(zeroed_g) > threshold_g)[0]

        if len(trigger_indices) > 0:
            t0_idx = trigger_indices[0]
            # 트리거 시점 이전 10ms 정도 여유를 두고 자르거나 이동하면 더 좋음 (Pre-trigger)
            # 여기서는 0초로 딱 맞춤
            t0_time = time_s[t0_idx]
            shifted_time = time_s - t0_time
        else:
            shifted_time = time_s

        return shifted_time, zeroed_g

    def get_clean_pulse_data(self) -> Dict:
        """메인 실행 함수"""
        channel = self.find_vehicle_accel_channel()
        if not channel:
            return {"error": "No suitable X-axis accelerometer found."}

        try:
            raw_g = channel[:]
            time_s = channel.time_track()
        except Exception as e:
            return {"error": f"Read Error: {e}"}

        clean_time, clean_g = self.preprocess_signal(time_s, raw_g)

        return {
            "time_s": clean_time,
            "accel_g": clean_g,
            "fs": 1.0 / (time_s[1] - time_s[0]) if len(time_s) > 1 else 10000.0,
            "meta": channel.properties,
            "sensor_name": channel.name,
            "sensor_loc": channel.properties.get("SENLOCD", "Unknown"),
        }
