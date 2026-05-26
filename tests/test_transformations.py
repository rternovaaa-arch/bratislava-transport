"""
Tests for GTFS transformation logic.
Uses plain Python instead of Spark for fast, reliable unit tests.
"""

import pytest


def map_route_type(route_type):
    """Map GTFS route_type number to human-readable name."""
    mapping = {0: "tram", 3: "bus", 11: "trolleybus"}
    return mapping.get(route_type, "other")


def normalize_hour(gtfs_time):
    """Extract and normalize hour from GTFS time string (handles times > 24:00)."""
    hour = int(gtfs_time.split(":")[0])
    return hour % 24


def count_unique(items):
    """Count unique items in a list."""
    return len(set(items))


def has_nulls(values):
    """Check if a list contains any None values."""
    return any(v is None for v in values)


# ── Tests ──────────────────────────────────────────────────────────────────

def test_route_type_mapping():
    """Test that route_type numbers are correctly mapped to names."""
    assert map_route_type(0) == "tram"
    assert map_route_type(3) == "bus"
    assert map_route_type(11) == "trolleybus"
    assert map_route_type(99) == "other"


def test_hour_extraction_normal():
    """Test hour extraction for normal daytime hours."""
    assert normalize_hour("06:30:00") == 6
    assert normalize_hour("14:45:00") == 14
    assert normalize_hour("23:59:00") == 23


def test_hour_extraction_gtfs_night():
    """Test that GTFS times over 24:00 are normalized correctly."""
    assert normalize_hour("25:10:00") == 1   # 25 % 24 = 1
    assert normalize_hour("24:00:00") == 0   # 24 % 24 = 0
    assert normalize_hour("26:30:00") == 2   # 26 % 24 = 2


def test_unique_stops_no_duplicates():
    """Test that duplicate stops are counted only once."""
    stops = ["stop_A", "stop_B", "stop_A"]  # stop_A appears twice
    assert count_unique(stops) == 2


def test_unique_stops_all_unique():
    """Test counting when all stops are unique."""
    stops = ["stop_A", "stop_B", "stop_C"]
    assert count_unique(stops) == 3


def test_null_detection_finds_nulls():
    """Test that null detection correctly identifies missing values."""
    values = ["9300001", "9300002", None]
    assert has_nulls(values) == True


def test_null_detection_clean_data():
    """Test that null detection passes for clean data."""
    values = ["9300001", "9300002", "9300003"]
    assert has_nulls(values) == False