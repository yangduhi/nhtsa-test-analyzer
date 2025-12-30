"""
Batch Crash Pulse Analysis Script (Production).
Process ALL downloaded files and save advanced metrics to SQLite.
"""

import os
import sqlite3
import pandas as pd
from tqdm import tqdm

from config import settings
from src.analysis.pulse import CrashPulseAnalyzer
from src.analysis.pipeline import CrashAnalysisPipeline
from src.analysis.metrics.kinematics import BasicKinematics
from src.analysis.metrics.dynamics import MaxDisplacement, EnergyAnalysis, OLCCalculator


def init_analysis_table():
    """분석 결과를 저장할 테이블 생성 (기존 DB 유지)"""
    conn = sqlite3.connect(settings.DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pulse_metrics (
            test_no INTEGER PRIMARY KEY,
            peak_g REAL,
            time_at_peak_ms REAL,
            delta_v_kph REAL,
            max_crush_mm REAL,
            time_at_max_crush_ms REAL,
            olc_approx_g REAL,
            specific_energy_j_kg REAL,
            total_energy_kj REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(test_no) REFERENCES crash_tests(test_no)
        )
    """)
    conn.commit()
    conn.close()


def save_results_to_db(results_list):
    """분석 결과를 DB에 Upsert"""
    if not results_list:
        return

    conn = sqlite3.connect(settings.DB_PATH)
    cursor = conn.cursor()

    data = []
    for r in results_list:
        data.append(
            (
                r["test_no"],
                r.get("Peak_G"),
                r.get("Time_at_Peak_ms"),
                r.get("Delta_V_kph"),
                r.get("Max_Dynamic_Crush_mm"),
                r.get("Time_at_Max_Crush_ms"),
                r.get("OLC_Approx_G"),
                r.get("Specific_Energy_Absorbed_J_kg"),
                r.get("Total_Energy_Absorbed_kJ"),
            )
        )

    cursor.executemany(
        """
        INSERT OR REPLACE INTO pulse_metrics 
        (test_no, peak_g, time_at_peak_ms, delta_v_kph, max_crush_mm, 
         time_at_max_crush_ms, olc_approx_g, specific_energy_j_kg, total_energy_kj)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        data,
    )

    conn.commit()
    conn.close()
    print(f"[*] Saved {len(results_list)} analysis results to DB.")


def get_ready_tests():
    conn = sqlite3.connect(settings.DB_PATH)
    # LIMIT 제거: 모든 데이터 조회
    query = """
    SELECT 
        t.test_no, t.year, t.make, t.model, v.weight, q.filename
    FROM crash_tests t
    JOIN download_queue q ON t.test_no = q.test_no
    JOIN test_vehicles v ON t.test_no = v.test_no
    WHERE t.crash_type = 'VEHICLE INTO BARRIER'
      AND q.file_type = 'TDMS' 
      AND q.status = 'DONE'
      AND v.weight IS NOT NULL
    ORDER BY t.test_no DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def find_tdms_file(test_no, zip_filename):
    base_dir = os.path.join(settings.DATA_ROOT, "downloads", str(test_no))
    if not os.path.exists(base_dir):
        return None
    for root, _, files in os.walk(base_dir):
        for f in files:
            if f.lower().endswith(".tdms"):
                return os.path.join(root, f)
    return None


def main():
    # 1. 결과 테이블 준비
    init_analysis_table()

    print("[*] Loading all test cases from DB...")
    df_tests = get_ready_tests()

    if df_tests.empty:
        print("[!] No ready tests found.")
        return

    print(f"[*] Found {len(df_tests)} tests. Starting Batch Analysis...")

    # 파이프라인 설정
    pipeline = CrashAnalysisPipeline()
    pipeline.add_metric(BasicKinematics())
    pipeline.add_metric(MaxDisplacement())
    pipeline.add_metric(OLCCalculator())
    pipeline.add_metric(EnergyAnalysis())

    results_buffer = []

    # TQDM으로 진행률 표시
    for _, row in tqdm(df_tests.iterrows(), total=len(df_tests)):
        test_no = row["test_no"]
        veh_weight = row["weight"]

        tdms_path = find_tdms_file(test_no, row["filename"])
        if not tdms_path:
            continue

        analyzer = CrashPulseAnalyzer(tdms_path)
        clean_data = analyzer.get_clean_pulse_data()

        if "error" in clean_data:
            # 에러 발생 시 건너뜀 (로그 생략하여 속도 향상)
            continue

        try:
            res = pipeline.run(
                time_data=clean_data["time_s"],
                accel_data=clean_data["accel_g"],
                vehicle_weight=veh_weight,
            )
            res["test_no"] = test_no
            results_buffer.append(res)
        except Exception:
            continue

        # 메모리 관리를 위해 50개마다 DB 저장
        if len(results_buffer) >= 50:
            save_results_to_db(results_buffer)
            results_buffer = []

    # 남은 데이터 저장
    if results_buffer:
        save_results_to_db(results_buffer)

    print("\n[Done] Batch analysis completed.")


if __name__ == "__main__":
    main()
