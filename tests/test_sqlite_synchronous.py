import sqlite3

import pytest

from vanth.migrations import configure_connection, synchronous_level

PRAGMA_VALUES = {"OFF": 0, "NORMAL": 1, "FULL": 2, "EXTRA": 3}


@pytest.mark.parametrize(("value", "expected"), [
    ("OFF", 0), ("NORMAL", 1), ("FULL", 2), ("EXTRA", 3),
    ("normal", 1), (" Normal ", 1),
])
def test_valid_level_is_applied_per_connection(tmp_path, monkeypatch, value, expected):
    monkeypatch.setenv("VANTH_SQLITE_SYNCHRONOUS", value)
    db = sqlite3.connect(tmp_path / "jobs.sqlite")
    try:
        configure_connection(db)
        assert db.execute("PRAGMA synchronous").fetchone()[0] == expected
        assert db.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
    finally:
        db.close()


@pytest.mark.parametrize("value", ["", "OFFF", "WAL", "1", "2", "full\nnormal", "auto"])
def test_unknown_level_falls_back_to_full(tmp_path, monkeypatch, value):
    monkeypatch.setenv("VANTH_SQLITE_SYNCHRONOUS", value)
    db = sqlite3.connect(tmp_path / "jobs.sqlite")
    try:
        configure_connection(db)
        assert db.execute("PRAGMA synchronous").fetchone()[0] == PRAGMA_VALUES["FULL"]
    finally:
        db.close()


def test_unset_level_is_full(tmp_path, monkeypatch):
    monkeypatch.delenv("VANTH_SQLITE_SYNCHRONOUS", raising=False)
    assert synchronous_level() == "FULL"
    db = sqlite3.connect(tmp_path / "jobs.sqlite")
    try:
        configure_connection(db)
        assert db.execute("PRAGMA synchronous").fetchone()[0] == PRAGMA_VALUES["FULL"]
    finally:
        db.close()
