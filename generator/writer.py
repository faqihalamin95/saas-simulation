import os
from datetime import datetime

import pandas as pd

from .config import BASE_OUTPUT_PATH


def ensure_directory(path: str):
    os.makedirs(path, exist_ok=True)


def write_parquet(events: list, dataset_name: str, ts_field="event_timestamp_local"):
    """
    Write events into partitioned parquet by ts_field (default: event_timestamp_local).
    Compatible with Layer 0 schema.
    """

    if not events:
        print("No data to write.")
        return

    df = pd.DataFrame(events)

    if ts_field not in df.columns:
        raise ValueError(f"{ts_field} column is required in the events")

    df[ts_field] = pd.to_datetime(df[ts_field])
    df["event_date"] = df[ts_field].dt.date

    for event_date, group in df.groupby("event_date"):

        partition_path = os.path.join(
            BASE_OUTPUT_PATH,
            dataset_name,
            f"event_date={event_date}"
        )

        ensure_directory(partition_path)

        file_name = f"{dataset_name}_{int(datetime.utcnow().timestamp())}.parquet"
        full_path = os.path.join(partition_path, file_name)

        group.drop(columns=["event_date"]).to_parquet(
            full_path,
            index=False,
            engine="pyarrow"
        )

    print(f"[{dataset_name}] Written {len(df)} records.")
