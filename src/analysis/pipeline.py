"""
Analysis Pipeline Manager.
Orchestrates the signal processing and metric calculation.
"""

from typing import List, Dict, Any
from src.analysis.processing import SignalProcessor
from src.analysis.metrics.base import MetricStrategy


class CrashAnalysisPipeline:
    def __init__(self):
        self.metrics: List[MetricStrategy] = []

    def add_metric(self, metric: MetricStrategy):
        """분석 전략(Metric) 추가"""
        self.metrics.append(metric)

    def run(self, time_data, accel_data, vehicle_weight=None) -> Dict[str, Any]:
        """
        Raw Data를 받아 처리하고 모든 메트릭을 계산
        """
        # 1. 신호 처리 (CFC 60 필터링 & 적분)
        signal = SignalProcessor.process(time_data, accel_data, cfc=60)

        results = {
            "signal_obj": signal  # 시각화를 위해 신호 객체 자체도 반환
        }

        # 2. 등록된 메트릭 순차 실행
        for metric in self.metrics:
            # 동적으로 차량 중량 주입 (필요한 경우)
            if vehicle_weight and "vehicle_mass" not in metric.params:
                metric.params["vehicle_mass"] = vehicle_weight

            # 계산 결과 병합
            try:
                metric_res = metric.calculate(signal)
                results.update(metric_res)
            except Exception as e:
                results[f"Error_{metric.__class__.__name__}"] = str(e)

        return results
