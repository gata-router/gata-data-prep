"""Tests for the utility functions."""

__author__ = "Dave Hall <me@davehall.com.au>"
__copyright__ = "Copyright 2024 - 2026, Skwashd Services Pty Ltd https://gata.works"
__license__ = "MIT"

import datetime
import json
import os
import typing

import boto3
import moto
import pandas as pd
import pytest

import src.utils as utils


@pytest.fixture(autouse=True)
def _setup_environment() -> None:
    """Set up the environment variables."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"  # noqa: S105 This is a fake value for the tests


def test_count_labels_single_label() -> None:
    """Test count_labels with a single label."""
    data = {"label": ["A", "A", "A"]}
    df = pd.DataFrame(data)
    result = utils.count_labels(df)
    assert result["Count"][0] == 3


def test_count_labels_multiple_labels() -> None:
    """Test count_labels with multiple labels."""
    data = {"label": ["A", "B", "A", "C", "B", "A"]}
    df = pd.DataFrame(data)
    result = utils.count_labels(df)
    expected = "  Label  Count\n0     A      3\n1     B      2\n2     C      1"
    assert str(result) == expected


def test_count_labels_no_labels() -> None:
    """Test count_labels with no labels."""
    data: dict[str, list[str]] = {"label": []}
    df = pd.DataFrame(data)
    result = utils.count_labels(df)
    assert result.empty


def test_get_end_datetime() -> None:
    """Test get_end_datetime function."""
    batch_id = "20250111"
    expected = datetime.datetime(2025, 1, 10, 23, 59, 59, 999999, tzinfo=datetime.UTC)
    result = utils.get_end_datetime(batch_id)
    assert result == expected


@pytest.mark.parametrize(
    ("batch_id", "expected_exception"),
    [
        ("0", ValueError),
        ("-1", ValueError),
        ("string", ValueError),
    ],
)
def test_get_end_datetime_garbage(
    batch_id: str, expected_exception: type[Exception]
) -> None:
    """Test get_end_datetime function with garbage values."""
    with pytest.raises(expected_exception):
        utils.get_end_datetime(batch_id)


@pytest.mark.parametrize(
    ("batch_id", "expected_exception"),
    [
        ("0", ValueError),
        ("-1", ValueError),
    ],
)
def test_get_start_datetime_garbage_batch(
    batch_id: str, expected_exception: type[Exception]
) -> None:
    """Test get_start_datetime function."""
    with pytest.raises(expected_exception):
        utils.get_start_datetime(batch_id, 10)


@pytest.mark.parametrize(
    ("offset", "expected_exception"),
    [
        ("string", TypeError),
    ],
)
def test_get_start_datetime_garbage_range(
    offset: typing.Any,  # noqa: ANN401 We need to pass garbage values
    expected_exception: type[Exception],
) -> None:
    """Test get_start_datetime function."""
    with pytest.raises(expected_exception):
        utils.get_start_datetime("20250101", offset)


@moto.mock_aws
def test_load_secret() -> None:
    """Test load_secret function."""
    secret = json.dumps(
        {
            "engine": "postgres",
            "host": "example.87v7w8vyv8x29j9lueft.us-east-1.rds.amazonaws.com",
            "username": "example",
            "password": "##not_really_a_password123##",
            "dbname": "example",
            "cluster_arn": "arn:aws:rds:us-east-1:123456789012:cluster:example",
            "dbClusterIdentifier": "87v7w8vyv8x29j9lueft",
        }
    )
    sm_client = boto3.client("secretsmanager")
    sm_client.create_secret(
        Name="db-creds",
        SecretString=secret,
    )
    result = utils.load_secret("db-creds")
    assert result == json.loads(secret)


@pytest.mark.filterwarnings(
    "ignore::DeprecationWarning"
)  # "datetime.datetime.utcnow() is deprecated" coming from boto3
@moto.mock_aws
def test_write_to_s3_json() -> None:
    """Test that data is written to S3 correctly."""
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket="bucket")

    data = {"key": "value"}
    df = pd.DataFrame(data, index=[0])
    utils.write_to_s3_json("bucket", "folder/data.json", df)

    response = s3.get_object(Bucket="bucket", Key="folder/data.json")
    assert response["Body"].read().decode("utf-8") == '{"key":"value"}\n'
