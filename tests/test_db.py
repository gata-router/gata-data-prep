"""Test AWS RDS Data API Client."""

__author__ = "Dave Hall <me@davehall.com.au>"
__copyright__ = "Copyright 2024 - 2026, Skwashd Services Pty Ltd https://gata.works"
__license__ = "MIT"

import os
from unittest.mock import MagicMock, patch

import moto
import pytest

from src.db import DbClient

"""Tests for the database client."""


@pytest.fixture(autouse=True)
def _setup_aws_credentials() -> None:
    """Configure AWS credentials for tests."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"  # noqa: S105 This is a fake value for the tests
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def db_client_params() -> dict[str, str]:
    """Return standard parameters for DbClient initialization."""
    return {
        "cluster_arn": "arn:aws:rds:us-east-1:123456789012:cluster:test",
        "secret_arn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:test",
        "db_name": "test_db",
    }


@pytest.mark.filterwarnings(
    "ignore::DeprecationWarning"
)  # "datetime.datetime.utcnow() is deprecated" coming from boto3
@moto.mock_aws
def test_db_client_init_success(db_client_params: dict[str, str]) -> None:
    """Test successful initialization of DbClient."""
    with patch("boto3.client") as mock_boto3:
        mock_rds = MagicMock()
        mock_boto3.return_value = mock_rds

        client = DbClient(**db_client_params)

        mock_boto3.assert_called_once_with("rds-data")
        mock_rds.execute_statement.assert_called_once()
        assert client._cluster_arn == db_client_params["cluster_arn"]
        assert client._secret_arn == db_client_params["secret_arn"]
        assert client._db_name == db_client_params["db_name"]


@pytest.mark.filterwarnings(
    "ignore::DeprecationWarning"
)  # "datetime.datetime.utcnow() is deprecated" coming from boto3
@moto.mock_aws
def test_db_client_init_resume_success(db_client_params: dict[str, str]) -> None:
    """Test successful initialization after database resume."""
    with patch("boto3.client") as mock_boto3:
        mock_rds = MagicMock()
        mock_rds.exceptions.DatabaseResumingException = Exception

        # Fail twice then succeed
        mock_rds.execute_statement.side_effect = [
            Exception(),
            Exception(),
            {"formattedRecords": "[]"},
        ]

        mock_boto3.return_value = mock_rds

        client = DbClient(**db_client_params)

        assert mock_rds.execute_statement.call_count == 3
        assert client._cluster_arn == db_client_params["cluster_arn"]


@pytest.mark.filterwarnings(
    "ignore::DeprecationWarning"
)  # "datetime.datetime.utcnow() is deprecated" coming from boto3
@moto.mock_aws
def test_db_client_init_resume_failure(db_client_params: dict[str, str]) -> None:
    """Test failed initialization after database fails to resume."""
    with patch("boto3.client") as mock_boto3:
        mock_rds = MagicMock()
        mock_rds.exceptions.DatabaseResumingException = Exception

        # Always fail
        mock_rds.execute_statement.side_effect = Exception()
        mock_boto3.return_value = mock_rds

        with pytest.raises(
            ConnectionError, match="The database took too long to resume"
        ):
            DbClient(**db_client_params)

        assert mock_rds.execute_statement.call_count == 10


@pytest.mark.filterwarnings(
    "ignore::DeprecationWarning"
)  # "datetime.datetime.utcnow() is deprecated" coming from boto3
@moto.mock_aws
def test_db_client_init_attributes(db_client_params: dict[str, str]) -> None:
    """Test that DbClient attributes are set correctly."""
    with patch("boto3.client") as mock_boto3:
        mock_rds = MagicMock()
        mock_boto3.return_value = mock_rds

        client = DbClient(**db_client_params)

        assert client._cluster_arn == db_client_params["cluster_arn"]
        assert client._secret_arn == db_client_params["secret_arn"]
        assert client._db_name == db_client_params["db_name"]
        assert client._rds == mock_rds


@pytest.mark.filterwarnings(
    "ignore::DeprecationWarning"
)  # "datetime.datetime.utcnow() is deprecated" coming from boto3
@moto.mock_aws
def test_db_client_select_success(db_client_params: dict[str, str]) -> None:
    """Test successful SELECT query execution."""
    client = DbClient(**db_client_params)

    # Patch just the execute_statement method since moto doesn't implement it
    with patch.object(client._rds, "execute_statement") as mock_execute:
        mock_execute.return_value = {"formattedRecords": '[{"id": 1, "name": "test"}]'}

        result = client.select(
            "SELECT id, name FROM table WHERE id = :id",
            [{"name": "id", "value": {"longValue": 1}}],
        )

        assert result == [{"id": 1, "name": "test"}]
        mock_execute.assert_called_with(
            resourceArn=db_client_params["cluster_arn"],
            secretArn=db_client_params["secret_arn"],
            database=db_client_params["db_name"],
            sql="SELECT id, name FROM table WHERE id = :id",
            parameters=[{"name": "id", "value": {"longValue": 1}}],
            resultSetOptions={
                "decimalReturnType": "DOUBLE_OR_LONG",
                "longReturnType": "LONG",
            },
            formatRecordsAs="JSON",
        )


@pytest.mark.filterwarnings(
    "ignore::DeprecationWarning"
)  # "datetime.datetime.utcnow() is deprecated" coming from boto3
@moto.mock_aws
def test_db_client_select_non_select_query(db_client_params: dict[str, str]) -> None:
    """Test that non-SELECT queries are rejected."""
    with patch("boto3.client") as mock_boto3:
        mock_rds = MagicMock()
        mock_boto3.return_value = mock_rds

        client = DbClient(**db_client_params)

        with pytest.raises(ValueError, match="Query must start with SELECT"):
            client.select("INSERT INTO table (id) VALUES (:id)", [])


@pytest.mark.filterwarnings(
    "ignore::DeprecationWarning"
)  # "datetime.datetime.utcnow() is deprecated" coming from boto3
@moto.mock_aws
def test_db_client_select_empty_result(db_client_params: dict[str, str]) -> None:
    """Test handling of empty result sets."""
    with patch("boto3.client") as mock_boto3:
        mock_rds = MagicMock()
        mock_rds.execute_statement.return_value = {"formattedRecords": "[]"}
        mock_boto3.return_value = mock_rds

        client = DbClient(**db_client_params)
        result = client.select("SELECT * FROM table WHERE 1=0", [])

        assert result == []


@pytest.mark.filterwarnings(
    "ignore::DeprecationWarning"
)  # "datetime.datetime.utcnow() is deprecated" coming from boto3
@moto.mock_aws
def test_db_client_select_with_params(db_client_params: dict[str, str]) -> None:
    """Test query execution with multiple parameters."""
    with patch("boto3.client") as mock_boto3:
        mock_rds = MagicMock()
        mock_rds.execute_statement.return_value = {"formattedRecords": '[{"count": 1}]'}
        mock_boto3.return_value = mock_rds

        client = DbClient(**db_client_params)
        params = [
            {"name": "start", "value": {"longValue": 1000}},
            {"name": "end", "value": {"longValue": 2000}},
        ]

        result = client.select(
            "SELECT COUNT(*) as count FROM table WHERE time BETWEEN :start AND :end",
            params,
        )

        assert result == [{"count": 1}]
        mock_rds.execute_statement.assert_called_with(
            resourceArn=db_client_params["cluster_arn"],
            secretArn=db_client_params["secret_arn"],
            database=db_client_params["db_name"],
            sql="SELECT COUNT(*) as count FROM table WHERE time BETWEEN :start AND :end",
            parameters=params,
            resultSetOptions={
                "decimalReturnType": "DOUBLE_OR_LONG",
                "longReturnType": "LONG",
            },
            formatRecordsAs="JSON",
        )
