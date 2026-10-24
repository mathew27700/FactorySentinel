"""Smoke test: the repository skeleton is in place."""

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

MAIN_FOLDERS = [
    "simulator",
    "streaming",
    "warehouse",
    "airflow",
    "monitoring",
    "tests",
    "docs",
    "scripts",
]


@pytest.mark.parametrize("folder", MAIN_FOLDERS)
def test_main_folder_exists(folder: str) -> None:
    assert (ROOT / folder).is_dir(), f"missing folder: {folder}/"


def test_docker_compose_exists() -> None:
    assert (ROOT / "docker-compose.yml").is_file()
