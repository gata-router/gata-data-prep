"""Client for AWS RDS Data API."""

__author__ = "Dave Hall <me@davehall.com.au>"
__copyright__ = "Copyright 2024 - 2026, Skwashd Services Pty Ltd https://gata.works"
__license__ = "MIT"


import json
import logging
import time
import typing

import boto3
from types_boto3_rds_data.client import RDSDataServiceClient


class DbClient:
    """Client for AWS RDS Data API."""

    def __init__(self, cluster_arn: str, secret_arn: str, db_name: str) -> None:
        """
        Initialize the database client.

        Args:
        ----
            cluster_arn: The ARN of the Aurora database cluster.
            secret_arn: The ARN of the secret for connecting to the db.
            db_name: The name of the database.

        """
        self._cluster_arn = cluster_arn
        self._secret_arn = secret_arn
        self._db_name = db_name
        self._rds: RDSDataServiceClient = boto3.client("rds-data")

        self._prime_connection()

    def _prime_connection(self) -> None:
        """
        Prime the connection to the database.

        Attempts to connect 5 times over the course of around a minute. This gives the
        database a reasonable amount of time to resume if it is suspended.
        """
        connected = False
        attempts = 0
        while attempts < 10:
            try:
                self._rds.execute_statement(
                    resourceArn=self._cluster_arn,
                    secretArn=self._secret_arn,
                    database=self._db_name,
                    sql="SELECT 1",
                    formatRecordsAs="JSON",
                )
                connected = True
                break
            except self._rds.exceptions.DatabaseResumingException:
                attempts += 1
                logging.info("Database is resuming, waiting 3 seconds")
                time.sleep(3)

        if not connected:
            raise ConnectionError("The database took too long to resume.")  # noqa: TRY003 This string is short and keeps the code simple

    def select(
        self, query: str, params: list[dict[str, typing.Any]]
    ) -> list[dict[str, typing.Any]]:
        """
        Execute a SELECT query.

        Args:
        ----
            query: The query to execute.
            params: The parameters to pass to the query.

        Returns:
        -------
            The records found by the query.

        """
        if not query.startswith("SELECT"):
            raise ValueError("Query must start with SELECT")  # noqa: TRY003 This string is short and keeps the code simple

        response = self._rds.execute_statement(
            resourceArn=self._cluster_arn,
            secretArn=self._secret_arn,
            database=self._db_name,
            sql=query,
            parameters=params,  # type: ignore[arg-type]
            resultSetOptions={
                "decimalReturnType": "DOUBLE_OR_LONG",
                "longReturnType": "LONG",
            },
            formatRecordsAs="JSON",
        )

        return json.loads(response["formattedRecords"])
