"""
Signal processing engine compliant with SAE J211.
Includes Drift Correction logic for accurate Displacement calculation.
"""

import numpy as np
from scipy import signal, integrate
from src.analysis.core import CrashSignal
import src


class SignalProcessor:
    """신호 처리 전용 클래스"""

    @staticmethod
    def process(time_s: np.ndarray, raw_g: np.ndarray, cfc: int = 60) -> CrashSignal:
        """
        Raw Data를 받아 필터링 및 적분을 수행하여 CrashSignal 객체 생성
        """
        # 1. Sampling Rate 계산
        if len(time_s) < 2:
            raise ValueError("Data too short")

        dt = time_s[1] - time_s[0]
        fs = 1.0 / dt

        # 2. CFC 필터링 (SAE J211)
        filtered_g = SignalProcessor.apply_cfc_filter(raw_g, fs, cfc)

        # [Drift Correction 1] 0점 미세 오프셋 제거
        # 충돌 전(T < 0) 구간의 데이터가 있다면, 그 구간의 평균을 구해 전체에서 뺍니다.
        pre_impact_mask = time_s < 0
        if np.any(pre_impact_mask):
            offset = np.mean(filtered_g[pre_impact_mask])
            filtered_g = filtered_g - offset

        # 3. 단위 변환 (G -> m/s^2)
        accel_mps2 = filtered_g * 9.80665

        # 4. 1차 적분 (가속도 -> 속도)
        if hasattr(integrate, "cumulative_trapezoid"):
            integ_func = integrate.cumulative_trapezoid
        else:
            integ_func = integrate.cumtrapz

        velocity_mps = integ_func(accel_mps2, time_s, initial=0)

        # [Drift Correction 2] 적분 종료 시점 클리핑 (핵심 로직)
        # 속도가 0이 되는 지점(최대 변형점) 이후의 적분을 중단합니다.

        # 충돌 시작 후 일정 시간(20ms) 이후부터 Zero Crossing 검색
        start_search_idx = int(0.02 * fs)
        if start_search_idx < len(velocity_mps):
            # 속도 부호가 바뀌는 지점 찾기
            signs = np.sign(velocity_mps[start_search_idx:])
            zero_crossings = np.where(np.diff(signs))[0]

            if len(zero_crossings) > 0:
                stop_idx = start_search_idx + zero_crossings[0]
                # 해당 시점 이후 속도를 0으로 강제하여 변위 고정
                velocity_mps[stop_idx:] = 0

        velocity_kph = velocity_mps * 3.6

        # 5. 2차 적분 (속도 -> 변위)
        displacement_m = integ_func(velocity_mps, time_s, initial=0)

        return CrashSignal(
            time_ms=time_s * 1000,
            raw_accel_g=raw_g,
            filtered_accel_g=filtered_g,
            velocity_kph=velocity_kph,
            displacement_m=displacement_m,
            sample_rate=fs,
        )

    @staticmethod
    def apply_cfc_filter(data: np.ndarray, fs: float, cfc: int) -> np.ndarray:
        """
        SAE J211 Recommended CFC Filter
        """
        if cfc == 60:
            cutoff = 100.0
        elif cfc == 180:
            cutoff = 300.0
        elif cfc == 600:
            cutoff = 1000.0
        elif cfc == 1000:
            cutoff = 1650.0
        else:
            cutoff = cfc * 1.667

        nyq = 0.5 * fs
        normal_cutoff = cutoff / nyq

        if normal_cutoff >= 1.0:
            normal_cutoff = 0.99

        b, a = signal.butter(2, normal_cutoff, btype="low", analog=False)
        return signal.filtfilt(b, a, data)
