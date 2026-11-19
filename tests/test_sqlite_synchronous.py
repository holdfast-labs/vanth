import sqlite3

import pytest

from vanth.migrations import configure_connection, synchronous_level

PRAGMA_VALUES = {"OFF": 0, "NORMAL": 1, "FULL": 2, "EXTRA": 3}


@pytest.mark.parametrize(("value", "expected"), [
    ("NORMAL", 1), ("FULL", 2), ("EXTRA", 3),
    ("normal", 1), (" Normal ", 1),
])
def test_valid_level_is_applied_per_connection(tmp_path, monkeypatch, value, expected):
    monkeypatch.setenv("VANTH_SQLITE_SYNCHRONOUS", value)
    db = sqlite3.connect(tmp_path / "jobs.sqlite")
    try:
        # Start from a different level so the assertion cannot pass by accident.
        db.execute(f"PRAGMA synchronous={1 if expected != 1 else 2}")
        configure_connection(db)
        assert db.execute("PRAGMA synchronous").fetchone()[0] == expected
        assert db.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
    finally:
        db.close()


@pytest.mark.parametrize("value", ["", "OFF", "off", "OFFF", "WAL", "1", "2", "full\nnormal", "auto"])
def test_unknown_level_falls_back_to_full(tmp_path, monkeypatch, value):
    monkeypatch.setenv("VANTH_SQLITE_SYNCHRONOUS", value)
    db = sqlite3.connect(tmp_path / "jobs.sqlite")
    try:
        # SQLite's compiled default is already FULL, so start at NORMAL:
        # otherwise this test passes even when the pragma is never set.
        db.execute("PRAGMA synchronous=NORMAL")
        configure_connection(db)
        assert db.execute("PRAGMA synchronous").fetchone()[0] == PRAGMA_VALUES["FULL"]
    finally:
        db.close()


def test_off_is_rejected_because_it_can_corrupt(tmp_path, monkeypatch):
    monkeypatch.setenv("VANTH_SQLITE_SYNCHRONOUS", "OFF")
    assert synchronous_level() == "FULL"


def test_unset_level_is_full(tmp_path, monkeypatch):
    monkeypatch.delenv("VANTH_SQLITE_SYNCHRONOUS", raising=False)
    assert synchronous_level() == "FULL"
    db = sqlite3.connect(tmp_path / "jobs.sqlite")
    try:
        # Start at NORMAL so this fails if configure_connection never sets it.
        db.execute("PRAGMA synchronous=NORMAL")
        configure_connection(db)
        assert db.execute("PRAGMA synchronous").fetchone()[0] == PRAGMA_VALUES["FULL"]
    finally:
        db.close()
