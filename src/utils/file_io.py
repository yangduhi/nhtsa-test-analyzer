"""File and directory I/O utility functions.

This module provides helper functions for common filesystem operations like
ensuring directories exist and creating structured paths for saving files.
"""

from pathlib import Path
from typing import List


def ensure_dirs(dir_paths: List[str]) -> None:
    """Ensures that a list of directories exist, creating them if necessary.

    Args:
        dir_paths: A list of string paths for the directories to check/create.
    """
    for d in dir_paths:
        path = Path(d)
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            print(f"[SYSTEM] Created directory: {d}")


def get_save_path(
    base_dir: str, year: int, test_num: int, filename: str
) -> Path:
    """Creates a hierarchical save path and returns it as a Path object.

    Example:
        `get_save_path("data/raw", 2024, 1234, "signal.csv")`
        will create `data/raw/2024/1234/` and return
        `Path("data/raw/2024/1234/signal.csv")`.

    Args:
        base_dir: The root directory for saving.
        year: The year to use for the subdirectory.
        test_num: The test number to use for the subdirectory.
        filename: The name of the file to be saved.

    Returns:
        A Path object representing the full, structured path to the file.
    """
    path = Path(base_dir) / str(year) / str(test_num)
    path.mkdir(parents=True, exist_ok=True)
    return path / filename
