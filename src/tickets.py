"""Functions for handling the raw ticket data."""

__author__ = "Dave Hall <me@davehall.com.au>"
__copyright__ = "Copyright 2024 - 2026, Skwashd Services Pty Ltd https://gata.works"
__license__ = "MIT"


import collections.abc
import datetime
import logging
import os
import typing

import src.db as db

LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL)
LOGGER = logging.getLogger(__name__)


def get_tickets(
    db_client: db.DbClient,
    start: datetime.datetime,
    end: datetime.datetime,
    group_ids: list[int] | None = None,
) -> list[dict[str, typing.Any]]:
    """
    Get tickets from the database for a specified date range.

    Args:
    ----
        db_client: The database client to use.
        start: The start of the date range.
        end: The end of the date range.
        group_ids: A list of group IDs to limit the query.

    Returns:
    -------
        A list of Zendesk ticket dicts.

    """
    tickets: list[dict[str, typing.Any]] = []
    for ticket in _load_tickets(db_client, start, end, group_ids):
        tickets.append(ticket)
    return tickets


def _load_tickets(
    db_client: db.DbClient,
    start: datetime.datetime,
    end: datetime.datetime,
    group_ids: list[int] | None = None,
) -> collections.abc.Generator[dict[str, typing.Any]]:
    """
    Load tickets from database for specified date range.

    Use get_tickets() rather than _load_tickets(). We use this
    function to save some memory.

    Args:
    ----
        db_client: The database client to use.
        start: The start of the date range.
        end: The end of the date range.
        group_ids: A list of group IDs to limit the query.

    Returns:
    -------
        A generator that yields a Zendesk ticket.

    """
    limit = 50

    query = [
        "SELECT id, processed_data AS text, closed_group_id_mapped AS label",
        "FROM ticket",
        "WHERE (via_channel != 'api' OR (via_channel = 'api' AND initial_group_id = 0)) AND routed_by != 'gata-mapped' AND closed BETWEEN :start AND :end",
    ]

    params: list[dict[str, typing.Any]] = [
        {"name": "offset", "value": {"longValue": 0}},
        {"name": "limit", "value": {"longValue": limit}},
        {"name": "start", "value": {"longValue": int(start.timestamp())}},
        {"name": "end", "value": {"longValue": int(end.timestamp())}},
    ]

    if group_ids:
        indices = []
        for idx, val in enumerate(group_ids):
            indices.append(idx)
            params.append(
                {
                    "name": f"group_id_{idx}",
                    "value": {"longValue": int(val)},
                }
            )
        group_tokens = ", ".join([f":group_id_{int(i)}" for i in indices])
        query.append(f"AND closed_group_id_mapped IN ({group_tokens})")

    query.append("ORDER BY closed DESC")
    query.append("LIMIT :limit OFFSET :offset")

    sql = " ".join(query)
    LOGGER.debug("Query: %s", sql)

    while True:
        records = db_client.select(
            sql,
            params,
        )

        if not records:
            break

        params[0]["value"]["longValue"] += limit
        yield from records
