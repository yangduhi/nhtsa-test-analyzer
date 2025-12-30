# -*- coding: utf-8 -*-
"""
SQLite 데이터베이스와의 모든 상호작용을 관리하는 모듈.

이 모듈은 데이터베이스 연결, 테이블 생성, 데이터 저장 및 조회 등
데이터베이스 관련 모든 로직을 캡슐화한 `DatabaseHandler` 클래스를 제공합니다.

Classes:
    DatabaseHandler: SQLite DB 작업을 추상화한 핸들러 클래스.

DataClasses:
    DownloadTask: `download_queue` 테이블의 단일 행을 나타내는 데이터 클래스.
"""

import sqlite3
import json
import re
from typing import List, Set, Any
from dataclasses import dataclass
from loguru import logger

from config import settings
from src.core.models import NHTSARecord


@dataclass
class DownloadTask:
    """`download_queue` 테이블의 단일 작업을 나타내는 데이터 구조."""
    id: int
    test_no: int
    file_type: str
    url: str
    filename: str
    status: str


class DatabaseHandler:
    """
    SQLite 데이터베이스 작업을 관리하고 추상화하는 클래스.

    이 클래스는 테이블 스키마 초기화, 데이터 배치 저장, 중복 데이터 확인,
    다운로드 큐 관리 등 모든 DB 관련 기능을 포함합니다.

    Attributes:
        db_path (str): SQLite 데이터베이스 파일의 경로.
    """

    def __init__(self, db_path: str = settings.DB_PATH):
        """
        DatabaseHandler를 초기화하고 데이터베이스 스키마를 설정합니다.

        Args:
            db_path (str): 연결할 데이터베이스 파일의 경로.
        """
        self.db_path = db_path
        self._initialize_schema()

    def _get_connection(self) -> sqlite3.Connection:
        """
        SQLite 데이터베이스에 대한 새 연결을 생성하고 반환합니다.

        Returns:
            sqlite3.Connection: 데이터베이스 연결 객체.
        """
        return sqlite3.connect(self.db_path)

    def _initialize_schema(self) -> None:
        """
        데이터베이스에 필요한 모든 테이블과 인덱스를 생성합니다.
        테이블이 이미 존재할 경우, 아무 작업도 수행하지 않습니다.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS crash_tests (
                    test_no INTEGER PRIMARY KEY,
                    test_date TEXT,
                    make TEXT,
                    model TEXT,
                    year INTEGER,
                    body_type TEXT,
                    crash_type TEXT,
                    closing_speed REAL,
                    raw_json TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS test_vehicles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_no INTEGER,
                    vehicle_id INTEGER,
                    make TEXT,
                    model TEXT,
                    year INTEGER,
                    vin TEXT,
                    weight REAL,
                    wheelbase REAL,
                    length REAL,
                    width REAL,
                    vdi TEXT,
                    pdof REAL,
                    dpd_1 REAL, dpd_2 REAL, dpd_3 REAL, 
                    dpd_4 REAL, dpd_5 REAL, dpd_6 REAL,
                    measurements_pre TEXT,
                    measurements_post TEXT,
                    FOREIGN KEY(test_no) REFERENCES crash_tests(test_no)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS test_occupants (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_no INTEGER,
                    seat_pos TEXT,
                    type TEXT,
                    age INTEGER,
                    sex TEXT,
                    hic REAL,
                    chest_deflection REAL,
                    femur_left REAL,
                    femur_right REAL,
                    FOREIGN KEY(test_no) REFERENCES crash_tests(test_no)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS download_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_no INTEGER,
                    file_type TEXT,
                    url TEXT,
                    status TEXT DEFAULT 'PENDING',
                    filename TEXT,
                    FOREIGN KEY(test_no) REFERENCES crash_tests(test_no)
                )
            """)

            cursor.execute("CREATE INDEX IF NOT EXISTS idx_make_year ON crash_tests(make, year)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_crash_type ON crash_tests(crash_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_veh_test ON test_vehicles(test_no)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_occ_test ON test_occupants(test_no)")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_url ON download_queue(url)")
            
            conn.commit()
            
    def get_existing_ids(self) -> Set[int]:
        """
        데이터베이스에 이미 저장된 모든 테스트 ID를 조회하여 집합(Set)으로 반환합니다.

        Returns:
            Set[int]: 저장된 모든 고유 테스트 ID의 집합.
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT test_no FROM crash_tests")
                return {row[0] for row in cursor.fetchall()}
        except sqlite3.Error as e:
            logger.error(f"Failed to fetch existing IDs from database: {e}")
            return set()

    def _sanitize_filename(self, text: str) -> str:
        """
        문자열을 안전한 파일명으로 변환합니다.

        Args:
            text (str): 변환할 원본 문자열.

        Returns:
            str: 안전하게 변환된 파일명.
        """
        text = str(text).strip()
        text = re.sub(r'[\\/*?"<>|]', "", text)  # 윈도우 파일명 금지 문자 제거
        text = text.replace(" ", "_")  # 공백을 언더바(_)로 치환
        return text

    def save_records(self, records: List[NHTSARecord]) -> None:
        """
        여러 NHTSARecord 객체를 데이터베이스 트랜잭션 내에 배치 저장합니다.

        Args:
            records (List[NHTSARecord]): 저장할 NHTSARecord 객체의 리스트.
        """
        if not records:
            return

        with self._get_connection() as conn:
            try:
                cursor = conn.cursor()

                test_batch = []
                vehicle_batch = []
                occupant_batch = []
                download_batch = []

                for rec in records:
                    t = rec.test_info

                    # 1. Main Table Data for crash_tests
                    main_veh = rec.vehicles[0] if rec.vehicles else None
                    main_make = main_veh.make if main_veh else "UNKNOWN"
                    main_model = main_veh.model if main_veh else "UNKNOWN"
                    main_year = main_veh.year if (main_veh and main_veh.year is not None) else 0
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
                            rec.model_dump_json(by_alias=True), # 원본 JSON 전체 저장
                        )
                    )

                    # 2. Vehicle Details for test_vehicles
                    for v in rec.vehicles:
                        v_year = v.year if v.year is not None else 0
                        pre_json = json.dumps(v.pre_impact_points) if v.pre_impact_points else None
                        post_json = json.dumps(v.post_impact_points) if v.post_impact_points else None

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
                                v.dpd1, v.dpd2, v.dpd3, v.dpd4, v.dpd5, v.dpd6,
                                pre_json,
                                post_json,
                            )
                        )

                    # 3. Occupant Details for test_occupants
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

                    # 4. Prepare Download URLs for download_queue

                    # (1) TDMS data (often provided as v{test_id}.zip)
                    if rec.urls and rec.urls.url_tdms:
                        filename = f"v{t.test_id:05d}.zip"
                        download_batch.append(
                            (t.test_id, "TDMS", rec.urls.url_tdms, filename)
                        )

                    # (2) PDF Reports
                    if rec.reports:
                        # Helper to parse file size for sorting
                        def get_file_size(r: Any) -> int:
                            try:
                                return int(r.filesize) if r.filesize else 0
                            except ValueError:
                                return 0

                        # Sort reports by size in descending order (largest first)
                        sorted_reports = sorted(rec.reports, key=get_file_size, reverse=True)

                        for idx, report in enumerate(sorted_reports):
                            if not report.url:
                                continue

                            # For the largest (first) PDF, use the requested naming format
                            if idx == 0:
                                safe_make = self._sanitize_filename(main_make)
                                safe_model = self._sanitize_filename(main_model)
                                # Format: v{TestID}_{Year}_{Make}_{Model}.pdf (e.g., v10005_2017_CHEVROLET_VOLT.pdf)
                                filename = f"v{t.test_id:05d}_{main_year}_{safe_make}_{safe_model}.pdf"
                            else:
                                # For other supplementary PDFs, keep the original filename
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
                    f"Saved {len(records)} tests (Vehicles: {len(vehicle_batch)}, Occupants: {len(occupant_batch)}, Downloads: {len(download_batch)})."
                )

            except sqlite3.Error as e:
                logger.error(f"DB Transaction Failed: {e}")
                conn.rollback()
