from pathlib import Path
from typing import List


def ensure_dirs(dirs: List[str]):
    """디렉토리 존재 확인 및 생성"""
    for d in dirs:
        path = Path(d)
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            print(f"[SYSTEM] Created: {d}")


def get_save_path(base_dir: str, year: int, test_num: int, filename: str) -> Path:
    """계층적 저장 경로 생성 (data/raw/2024/1234/signal.csv)"""
    path = Path(base_dir) / str(year) / str(test_num)
    path.mkdir(parents=True, exist_ok=True)
    return path / filename
