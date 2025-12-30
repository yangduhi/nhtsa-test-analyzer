import sqlite3
import pandas as pd
import os

# DB 경로 설정 (환경에 맞게 수정 필요 시 변경)
DB_PATH = "data/nhtsa_data.db"


def inspect_db():
    if not os.path.exists(DB_PATH):
        print(f"[!] 데이터베이스 파일을 찾을 수 없습니다: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print(f"=== [1] 데이터베이스 요약 ({DB_PATH}) ===")
    tables = ["crash_tests", "test_vehicles", "test_occupants", "download_queue"]
    for table in tables:
        try:
            count = cursor.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            print(f"  - {table:<15}: {count:>5} records")
        except sqlite3.OperationalError:
            print(f"  - {table:<15}: [테이블 없음]")

    print("\n=== [2] 차량 상세 제원 및 파손 데이터 (test_vehicles) ===")
    print(
        "  * 확인 항목: 중량(Weight), 휠베이스, 전장/전폭, 파손지수(VDI), 충돌방향(PDOF)"
    )
    query_veh = """
    SELECT 
        test_no, make, model, year,
        weight, wheelbase, length, width,
        vdi, pdof, dpd_1
    FROM test_vehicles
    WHERE weight IS NOT NULL  -- 데이터가 있는 행만 우선 조회
    ORDER BY test_no DESC
    LIMIT 3
    """
    try:
        df_veh = pd.read_sql_query(query_veh, conn)
        if not df_veh.empty:
            print(df_veh.to_string(index=False))
        else:
            print("  [!] 아직 상세 제원 데이터가 수집되지 않았습니다.")
    except Exception as e:
        print(f"  [!] 쿼리 오류: {e}")

    print("\n=== [3] 탑승자 상해 결과 데이터 (test_occupants) ===")
    print(
        "  * 확인 항목: HIC(두부상해), Chest Deflection(흉부압박), Femur Load(대퇴부)"
    )
    query_occ = """
    SELECT 
        test_no, seat_pos, type,
        hic, chest_deflection as 'chest_mm', 
        femur_left as 'l_femur', femur_right as 'r_femur'
    FROM test_occupants
    WHERE hic IS NOT NULL OR chest_deflection IS NOT NULL
    ORDER BY test_no DESC
    LIMIT 3
    """
    try:
        df_occ = pd.read_sql_query(query_occ, conn)
        if not df_occ.empty:
            print(df_occ.to_string(index=False))
        else:
            print("  [!] 아직 상해 결과 데이터가 수집되지 않았습니다.")
    except Exception as e:
        print(f"  [!] 쿼리 오류: {e}")

    conn.close()


if __name__ == "__main__":
    inspect_db()
