"""Utility functions for data preparation."""

__author__ = "Dave Hall <me@davehall.com.au>"
__copyright__ = "Copyright 2024 - 2026, Skwashd Services Pty Ltd https://gata.works"
__license__ = "MIT"


import datetime
import json
import logging
import os

import boto3
import pandas as pd
from types_boto3_secretsmanager.client import SecretsManagerClient

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL)
LOGGER = logging.getLogger(__name__)


def count_labels(df: pd.DataFrame) -> pd.DataFrame:
    """
    Count the number of records for each label in the DataFrame.

    Args:
    ----
        df: The DataFrame containing the data.

    Returns:
    -------
        A DataFrame containing the instance counts for each label.

    """
    counts = df["label"].value_counts()

    groups = pd.DataFrame({"label": counts.index, "count": counts.values})
    groups.sort_values("label", inplace=True)
    groups.columns = ["Label", "Count"]
    return groups


def get_end_datetime(batch_id: str) -> datetime.datetime:
    """
    Get the final day in the range for the ticket data based on the batch ID.

    Args:
    ----
        batch_id: The ID of the batch.

    Returns:
    -------
        The final day in the range for the ticket data.

    """
    batch_datetime = datetime.datetime.strptime(f"{batch_id}0000Z", "%Y%m%d%H%M%S%z")

    return (batch_datetime - datetime.timedelta(days=1)).replace(
        hour=23, minute=59, second=59, microsecond=999999
    )


def get_start_datetime(batch_id: str, offset: int) -> datetime.datetime:
    """
    Get the final day in the range for the ticket data based on the batch ID.

    Args:
    ----
        batch_id: The ID of the batch.
        offset: The offset in days from the end of the batch.

    Returns:
    -------
        The final day in the range for the ticket data.

    """
    batch_datetime = datetime.datetime.strptime(f"{batch_id}0000Z", "%Y%m%d%H%M%S%z")

    return batch_datetime - datetime.timedelta(days=(offset))


def load_secret(name: str) -> dict[str, str]:
    """
    Load a secret from AWS Secrets Manager.

    Args:
    ----
        name: The name of the secret.

    Returns:
    -------
        The value of the secret.

    """
    sm: SecretsManagerClient = boto3.client("secretsmanager")
    secret = sm.get_secret_value(SecretId=name)
    return json.loads(secret["SecretString"])


def write_to_s3_json(bucket: str, filename: str, dataframe: pd.DataFrame) -> None:
    """
    Write pandas dataframe to an S3 bucket as JSON.

    Args:
    ----
        bucket: The name of the bucket.
        filename: Path to object to use as key.
        dataframe: The dataframe to write to S3.

    """
    data = dataframe.to_json(orient="records", lines=True)

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=filename, Body=data)
