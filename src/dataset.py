"""Functions for managing data sets."""

__author__ = "Dave Hall <me@davehall.com.au>"
__copyright__ = "Copyright 2024 - 2026, Skwashd Services Pty Ltd https://gata.works"
__license__ = "MIT"


import pandas as pd
import sklearn.model_selection


def filter(df: pd.DataFrame, labels: list[str]) -> pd.DataFrame:
    """
    Filter the DataFrame to the list of specified labels.

    Args:
    ----
        df: The DataFrame to filter.
        labels: A list of labels to keep.

    Returns:
    -------
        The filtered DataFrame.

    """
    return df[df["label"].isin(labels)]


def find_threshold(df: pd.DataFrame, threshold: float) -> list[str]:
    """
    Find labels in the DataFrame that are below the specified threshold.

    Args:
    ----
        df: The DataFrame containing the data.
        threshold: The threshold value.

    Returns:
    -------
        A list of labels that are below the threshold.

    """
    label_counts = df["label"].value_counts(normalize=True)
    low_volume_labels = label_counts[label_counts < threshold]
    values = low_volume_labels.index.tolist()
    values.sort()
    return values


def split(df: pd.DataFrame, test_size: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split the DataFrame into training and test sets.

    Args:
    ----
        df: The DataFrame to split.
        test_size: The proportion of the data to include in the test set.

    Returns:
    -------
        A tuple containing the training and test DataFrames.

    """
    train_df, test_df = (
        sklearn.model_selection.train_test_split(  # type [pd.DataFrame, pd.DataFrame]
            df,
            test_size=test_size,
            stratify=df["label"],
            random_state=42,
        )
    )

    return train_df, test_df


def update_label(
    df: pd.DataFrame, old_labels: list[str], new_label: int
) -> pd.DataFrame:
    """
    Update a label in the DataFrame.

    Args:
    ----
        df: The DataFrame to update.
        old_labels: List of labels to replace.
        new_label: The new label to use.

    Returns:
    -------
        The updated DataFrame.

    """

    def substitute_label(x: str) -> int:
        return new_label if x in old_labels else int(x)

    df["label"] = df["label"].apply(substitute_label)
    return df
