"""Tests for the dataset functions."""

__author__ = "Dave Hall <me@davehall.com.au>"
__copyright__ = "Copyright 2024 - 2026, Skwashd Services Pty Ltd https://gata.works"
__license__ = "MIT"

import pandas as pd
import pytest

import src.dataset as dataset


@pytest.fixture
def dataframe() -> pd.DataFrame:
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "text": [
                "zero",
                "zero",
                "one",
                "one",
                "two",
                "two",
                "three",
                "three",
                "three",
                "three",
                "three",
                "three",
                "four",
                "four",
                "four",
                "four",
                "four",
                "four",
            ],
            "label": [
                "0",
                "0",
                "1",
                "1",
                "2",
                "2",
                "3",
                "3",
                "3",
                "3",
                "3",
                "3",
                "4",
                "4",
                "4",
                "4",
                "4",
                "4",
            ],
        }
    )


def test_filter(dataframe: pd.DataFrame) -> None:
    """Test the filter function."""
    expected = pd.DataFrame({"text": ["two", "two"], "label": ["2", "2"]}, index=[4, 5])

    filtered = dataset.filter(dataframe, ["2"])  # type: pd.DataFrame
    pd.testing.assert_frame_equal(filtered, expected)


def test_find_threshold(dataframe: pd.DataFrame) -> None:
    """Test the find_threshold function."""
    expected = ["0", "1", "2"]
    result = dataset.find_threshold(dataframe, 0.3)
    assert result == expected


def test_split(dataframe: pd.DataFrame) -> None:
    """Test the split function."""
    train, test = dataset.split(dataframe, 0.33)

    assert train.shape[0] == 12
    assert test.shape[0] == 6


def test_update_label(dataframe: pd.DataFrame) -> None:
    """Test the update_label function."""
    expected = pd.DataFrame(
        {
            "text": [
                "zero",
                "zero",
                "one",
                "one",
                "two",
                "two",
                "three",
                "three",
                "three",
                "three",
                "three",
                "three",
                "four",
                "four",
                "four",
                "four",
                "four",
                "four",
            ],
            "label": [
                99,
                99,
                99,
                99,
                2,
                2,
                3,
                3,
                3,
                3,
                3,
                3,
                4,
                4,
                4,
                4,
                4,
                4,
            ],
        }
    )

    updated = dataset.update_label(dataframe, ["0", "1"], 99)  # type: pd.DataFrame
    pd.testing.assert_frame_equal(updated, expected)
