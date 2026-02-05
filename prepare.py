#!/usr/bin/env python
"""Prepare the training dataset for the model."""

__author__ = "Dave Hall <me@davehall.com.au>"
__copyright__ = "Copyright 2024 - 2026, Skwashd Services Pty Ltd https://gata.works"
__license__ = "MIT"

import datetime
import logging
import os
import sys

import pandas as pd

import src.dataset as dataset
import src.db as db
import src.tickets as tickets
import src.utils as utils

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL)
LOGGER = logging.getLogger(__name__)

TEST_SPLIT = 0.1

DB_ARN: str = os.environ["DB_ARN"]
DB_SECRET_ARN: str = os.environ["DB_SECRET_ARN"]
LOW_VOLUME_FALLBACK_LABEL: int = int(os.environ["LOW_VOLUME_FALLBACK_LABEL"])
LOW_VOLUME_MIN_COUNT = 10
LOW_VOLUME_PERIOD: int = int(os.environ.get("LOW_VOLUME_PERIOD", "365"))
LOW_VOLUME_THRESHOLD: float = float(os.environ.get("LOW_VOLUME_THRESHOLD", "0.033"))
TARGET_BUCKET: str = os.environ["TARGET_BUCKET"]
TRAINING_PERIOD: int = int(os.environ.get("TRAINING_PERIOD", "90"))


def main(batch_id: str) -> None:
    """
    Prepare the training dataset for the model.

    Args:
    ----
        batch_id: The ID of the batch.

    Returns:
    -------
        None

    """
    if len(batch_id) != 10 or not batch_id.isdigit():
        raise ValueError("batch_id must be in YYYYMMDDHH format")  # noqa: TRY003

    start = utils.get_start_datetime(batch_id, TRAINING_PERIOD)
    end = utils.get_end_datetime(batch_id)

    LOGGER.info("Preparing dataset for batch ID %s", batch_id)
    LOGGER.info("  Start date: %s", start.isoformat())
    LOGGER.info("  End date:   %s", end.isoformat())

    secret = utils.load_secret(DB_SECRET_ARN)
    db_client = db.DbClient(DB_ARN, DB_SECRET_ARN, secret["dbname"])

    general_df = pd.DataFrame(tickets.get_tickets(db_client, start, end))
    if general_df.empty:
        LOGGER.warning("No ticket data found")
        return

    label_counts = utils.count_labels(general_df)
    low_volume_labels = dataset.find_threshold(general_df, LOW_VOLUME_THRESHOLD)
    large_enough = 0
    for label in low_volume_labels:
        if (
            int(label_counts.loc[label_counts["Label"] == label]["Count"].iloc[0])
            >= LOW_VOLUME_MIN_COUNT
        ):
            large_enough += 1

    low_volume_label_id = 0
    create_low_volume = large_enough >= 2
    if not create_low_volume:
        LOGGER.warning(
            "Not enough low volume labels with at least 10 records to create a low volume dataset"
        )
        low_volume_label_id = LOW_VOLUME_FALLBACK_LABEL

    LOGGER.info(
        "The following labels have low volume (less than %.2f%% of total):\n%s\n\nThese labels will be excluded from the dataset.",
        LOW_VOLUME_THRESHOLD * 100,
        low_volume_labels,
    )

    LOGGER.info("Total records: %d", len(general_df))
    LOGGER.info("Label distribution:\n%s", utils.count_labels(general_df))

    general_df = dataset.update_label(
        general_df, low_volume_labels, low_volume_label_id
    )

    general_train_df, general_test_df = dataset.split(general_df, TEST_SPLIT)

    LOGGER.info("Training set size: %d", len(general_train_df))
    LOGGER.info("Test set size: %d", len(general_test_df))
    LOGGER.info(
        "Training set label distribution:\n%s", utils.count_labels(general_train_df)
    )
    LOGGER.info("Test set label distribution:\n%s", utils.count_labels(general_test_df))

    train_path = f"training/gata-general/{batch_id}/train/data.json"
    test_path = f"training/gata-general/{batch_id}/test/data.json"

    utils.write_to_s3_json(TARGET_BUCKET, train_path, general_train_df)
    utils.write_to_s3_json(TARGET_BUCKET, test_path, general_test_df)

    logging.info("general training set saved to s3://%s/%s", TARGET_BUCKET, train_path)
    logging.info("general test set saved to s3://%s/%s", TARGET_BUCKET, test_path)

    if not create_low_volume:
        LOGGER.info("Skipping low volume dataset creation")
        return

    LOGGER.info(
        "Label distribution after reassigning low volume labels:\n%s",
        utils.count_labels(general_df),
    )

    low_vol_df = dataset.filter(general_df, low_volume_labels)

    # Free up some memory
    del general_df, general_train_df, general_test_df

    LOGGER.info("general training and test datasets saved to S3")

    low_vol_start = start - datetime.timedelta(days=LOW_VOLUME_PERIOD - TRAINING_PERIOD)
    low_vol_end = start
    low_vol_df = pd.concat(
        [
            low_vol_df,
            pd.DataFrame(tickets.get_tickets(db_client, low_vol_start, low_vol_end)),
        ],
        ignore_index=True,
    )
    if low_vol_df.empty:
        LOGGER.warning("No low volume ticket data found")
        return

    LOGGER.info("Low volume records: %d", len(low_vol_df))
    LOGGER.info("Low volume label distribution:\n%s", utils.count_labels(low_vol_df))

    # We need to remove any labels with fewer than 10 records to allow proper splitting
    low_vol_df = low_vol_df[low_vol_df["label"] >= 10]
    LOGGER.info("Low volume label distribution:\n%s", utils.count_labels(low_vol_df))

    low_vol_train_df, low_vol_test_df = dataset.split(low_vol_df, TEST_SPLIT)

    LOGGER.info("Training set size: %d", len(low_vol_train_df))
    LOGGER.info("Test set size: %d", len(low_vol_test_df))
    LOGGER.info(
        "Training set label distribution:\n%s", utils.count_labels(low_vol_train_df)
    )
    LOGGER.info("Test set label distribution:\n%s", utils.count_labels(low_vol_test_df))

    train_path = f"training/gata-low-vol/{batch_id}/train/data.json"
    test_path = f"training/gata-low-vol/{batch_id}/test/data.json"

    utils.write_to_s3_json(TARGET_BUCKET, train_path, low_vol_train_df)
    utils.write_to_s3_json(TARGET_BUCKET, test_path, low_vol_test_df)

    LOGGER.info(
        "Low volume training set saved to s3://%s/%s", TARGET_BUCKET, train_path
    )
    LOGGER.info("Low volume test set saved to s3://%s/%s", TARGET_BUCKET, test_path)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {os.path.basename(sys.argv[0])} <batch_id>")
        sys.exit(1)

    try:
        main(sys.argv[1])
        LOGGER.info("Execution successful")
    except Exception:
        LOGGER.exception("Execution failed")
        sys.exit(1)
