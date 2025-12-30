"""
Database handler for storing NHTSA test data using SQLite.
FINAL VERSION: Normalized schema for Tests, Vehicles (Specs/Crash), and Occupants (Injury).
Includes smart file naming for PDFs: v{TestID}_{Year}_{Make}_{Model}.pdf
"""

import sqlite3
import json
import re  # [추가] 파일명 정제용
from typing import List, Set
from loguru import logger
import config
from src.core.models import NHTSARecord


class DatabaseHandler:
    """
    SQLite 저장소 관리자.
    OLAP(분석)과 OLTP(수집) 패턴을 혼용한 하이브리드 스키마 사용.
    """

    def __init__(self, db_path: str = config.settings.DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """
        테이블 스키마 초기화 (3-Tier Structure + Download Queue)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 1. Main Metadata Table (테스트 기본 정보)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crash_tests (
                test_no INTEGER PRIMARY KEY,
                test_date TEXT,
                make TEXT,
                model TEXT,
                year INTEGER,
                body_type TEXT,
                crash_type TEXT,        -- 정면/측면 등 분류용
                closing_speed REAL,
                raw_json TEXT,          -- NoSQL처럼 전체 데이터 백업
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 2. Vehicle Details Table (차량 제원 및 파손 정보 - 1:N)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_vehicles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                test_no INTEGER,
                vehicle_id INTEGER,
                make TEXT,
                model TEXT,
                year INTEGER,
                vin TEXT,
                weight REAL,    -- 시험 중량
                wheelbase REAL, -- 휠베이스
                length REAL,    -- 전장
                width REAL,     -- 전폭
                vdi TEXT,       -- 파손 지수
                pdof REAL,      -- 충돌 방향
                dpd_1 REAL, dpd_2 REAL, dpd_3 REAL, 
                dpd_4 REAL, dpd_5 REAL, dpd_6 REAL, -- 파손 깊이
                measurements_pre TEXT,  -- JSON: 충돌 전 계측점
                measurements_post TEXT, -- JSON: 충돌 후 계측점
                FOREIGN KEY(test_no) REFERENCES crash_tests(test_no)
            )
        """)

        # 3. Occupant Details Table (탑승자 및 상해 결과 - 1:N)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_occupants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                test_no INTEGER,
                seat_pos TEXT,  -- 좌석 위치
                type TEXT,      -- 더미 타입
                age INTEGER,
                sex TEXT,
                hic REAL,              -- 두부 상해
                chest_deflection REAL, -- 흉부 압박
                femur_left REAL,       -- 좌측 대퇴부 하중
                femur_right REAL,      -- 우측 대퇴부 하중
                FOREIGN KEY(test_no) REFERENCES crash_tests(test_no)
            )
        """)

        # 4. Download Queue Table (파일 다운로드 관리)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS download_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                test_no INTEGER,
                file_type TEXT, -- 'TDMS', 'PDF', 'ZIP'
                url TEXT,
                status TEXT DEFAULT 'PENDING', -- 'PENDING', 'DONE', 'ERROR'
                filename TEXT,
                FOREIGN KEY(test_no) REFERENCES crash_tests(test_no)
            )
        """)

        # 인덱스 생성
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_make_year ON crash_tests(make, year)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_crash_type ON crash_tests(crash_type)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_veh_test ON test_vehicles(test_no)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_occ_test ON test_occupants(test_no)"
        )
        cursor.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_url ON download_queue(url)"
        )

        conn.commit()
        conn.close()

    def get_existing_ids(self) -> Set[int]:
        """이미 수집된 Test ID 목록 반환"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT test_no FROM crash_tests")
                return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"Failed to fetch existing IDs: {e}")
            return set()

    def _sanitize_filename(self, s: str) -> str:
        """파일명으로 사용할 수 없는 문자 제거 및 공백 처리"""
        # 1. 문자열 변환 및 양쪽 공백 제거
        s = str(s).strip()
        # 2. 윈도우 파일명 금지 문자 제거 (\ / : * ? " < > |)
        s = re.sub(r'[\\/*?:"<>|]', "", s)
        # 3. 공백을 언더바(_)로 치환
        s = s.replace(" ", "_")
        return s

    def save_records(self, records: List[NHTSARecord]) -> None:
        """
        Pydantic 모델 리스트를 받아 DB에 저장.
        """
        if not records:
            return

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()

            test_batch = []
            vehicle_batch = []
            occupant_batch = []
            download_batch = []

            for rec in records:
                t = rec.test_info

                # 1. Main Table Data
                main_veh = rec.vehicles[0] if rec.vehicles else None
                main_make = main_veh.make if main_veh else "UNKNOWN"
                main_model = main_veh.model if main_veh else "UNKNOWN"
                main_year = (
                    main_veh.year if (main_veh and main_veh.year is not None) else 0
                )
                main_body = main_veh.body_type if main_veh else None
                crash_type = t.crash_config if t.crash_config else "UNKNOWN"

                test_batch.append(
                    (
                        t.test_id,
                        t.test_date,
                        main_make,
                        main_model,
                        main_year,
                        main_body,
                        crash_type,
                        t.closing_speed,
                        rec.model_dump_json(by_alias=True),
                    )
                )

                # 2. Vehicles Data
                for v in rec.vehicles:
                    v_year = v.year if v.year is not None else 0
                    pre_json = (
                        json.dumps(v.pre_impact_points) if v.pre_impact_points else None
                    )
                    post_json = (
                        json.dumps(v.post_impact_points)
                        if v.post_impact_points
                        else None
                    )

                    vehicle_batch.append(
                        (
                            t.test_id,
                            v.vehicle_id,
                            v.make,
                            v.model,
                            v_year,
                            v.vin,
                            v.weight,
                            v.wheelbase,
                            v.length,
                            v.width,
                            v.vdi,
                            v.pdof,
                            v.dpd1,
                            v.dpd2,
                            v.dpd3,
                            v.dpd4,
                            v.dpd5,
                            v.dpd6,
                            pre_json,
                            post_json,
                        )
                    )

                # 3. Occupants Data
                for occ in rec.occupants:
                    occupant_batch.append(
                        (
                            t.test_id,
                            occ.seat_pos,
                            occ.type,
                            occ.age,
                            occ.sex,
                            occ.hic,
                            occ.chest_deflection,
                            occ.femur_left,
                            occ.femur_right,
                        )
                    )

                # 4. Download URLs 준비

                # (1) TDMS 데이터 (v00000.zip)
                if rec.urls and rec.urls.url_tdms:
                    filename = f"v{t.test_id:05d}.zip"
                    download_batch.append(
                        (t.test_id, "TDMS", rec.urls.url_tdms, filename)
                    )

                # (2) PDF 리포트
                if rec.reports:
                    # 파일 크기 파싱 헬퍼
                    def get_file_size(r):
                        try:
                            return int(r.filesize) if r.filesize else 0
                        except ValueError:
                            return 0

                    # 용량 기준 내림차순 정렬 (가장 큰 파일이 메인 리포트일 확률 높음)
                    sorted_reports = sorted(
                        rec.reports, key=get_file_size, reverse=True
                    )

                    for idx, report in enumerate(sorted_reports):
                        if not report.url:
                            continue

                        # [수정됨] 가장 큰 파일(첫 번째)은 요청하신 포맷으로 저장
                        if idx == 0:
                            # 안전한 파일명 생성 (공백 -> _, 특수문자 제거)
                            safe_make = self._sanitize_filename(main_make)
                            safe_model = self._sanitize_filename(main_model)

                            # 포맷: v{TestID}_{Year}_{Make}_{Model}.pdf
                            # 예: v10005_2017_CHEVROLET_VOLT.pdf
                            filename = f"v{t.test_id:05d}_{main_year}_{safe_make}_{safe_model}.pdf"
                        else:
                            # 나머지 부가적인 파일은 원본 이름 유지
                            filename = report.filename

                        download_batch.append((t.test_id, "PDF", report.url, filename))

            # Batch Insert Execution
            cursor.executemany(
                """
                INSERT OR REPLACE INTO crash_tests 
                (test_no, test_date, make, model, year, body_type, crash_type, closing_speed, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                test_batch,
            )

            cursor.executemany(
                """
                INSERT INTO test_vehicles 
                (test_no, vehicle_id, make, model, year, vin, 
                 weight, wheelbase, length, width, 
                 vdi, pdof, dpd_1, dpd_2, dpd_3, dpd_4, dpd_5, dpd_6, 
                 measurements_pre, measurements_post)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                vehicle_batch,
            )

            cursor.executemany(
                """
                INSERT INTO test_occupants
                (test_no, seat_pos, type, age, sex, hic, chest_deflection, femur_left, femur_right)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                occupant_batch,
            )

            cursor.executemany(
                """
                INSERT OR IGNORE INTO download_queue (test_no, file_type, url, filename)
                VALUES (?, ?, ?, ?)
            """,
                download_batch,
            )

            conn.commit()
            logger.success(
                f"Saved {len(records)} tests (Vehicles: {len(vehicle_batch)}, Occupants: {len(occupant_batch)})."
            )

        except Exception as e:
            logger.error(f"DB Transaction Failed: {e}")
            conn.rollback()
        finally:
            conn.close()

    def _connect(self):
        """외부 연결용 헬퍼"""
        return sqlite3.connect(self.db_path)
