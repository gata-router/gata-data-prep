"""Tests for the tickets functions."""

__author__ = "Dave Hall <me@davehall.com.au>"
__copyright__ = "Copyright 2024 - 2026, Skwashd Services Pty Ltd https://gata.works"
__license__ = "MIT"

import datetime
import typing

import src.tickets as tickets


def test_get_tickets() -> None:
    """Test fetching tickets."""

    # Mock DB client with sample response
    class MockDbClient:
        def __init__(self) -> None:
            self.cycle = 0

        def select(
            self, sql: str, params: list[dict[str, typing.Any]]
        ) -> list[dict[str, typing.Any]]:
            if self.cycle > 0:
                return []

            self.cycle += 1
            return [
                {
                    "id": "1",
                    "closed_group_id_mapped": "123456",
                    "processed_text": "ticket text",
                },
                {
                    "id": "2",
                    "closed_group_id_mapped": "654321",
                    "processed_text": "more text",
                },
            ]

    db_client = MockDbClient()

    start = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
    end = datetime.datetime(2025, 1, 2, tzinfo=datetime.UTC)

    result = list(tickets.get_tickets(db_client, start, end))  # type: ignore[arg-type]
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["id"] == "1"
    assert result[1]["id"] == "2"


def test_get_tickets_with_groups() -> None:
    """Test fetching tickets with a group constraint."""

    # Mock DB client with sample response
    class MockDbClient:
        def __init__(self) -> None:
            self.cycle = 0

        def select(
            self, sql: str, params: list[dict[str, typing.Any]]
        ) -> list[dict[str, typing.Any]]:
            if self.cycle > 0:
                return []

            self.cycle += 1
            return [
                {
                    "id": "3",
                    "closed_group_id_mapped": "123456",
                    "processed_text": "ticket text",
                }
            ]

    db_client = MockDbClient()

    start = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    end = datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC)

    result = list(tickets.get_tickets(db_client, start, end, [123456]))  # type: ignore[arg-type]
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["id"] == "3"
