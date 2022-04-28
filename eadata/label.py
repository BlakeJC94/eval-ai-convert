from pathlib import Path
import pickle
import logging

import pandas as pd
import numpy as np

from .globals import PATIENT_IDS, SPLIT_NAMES, TIMESTAMP_FORMAT
from .paths import PARQUET_PATH, SZTIMES_PATH, ARTIFACTS_PATH

logger = logging.getLogger(__name__)


def label(
    patient_id: str,
    forecast_window: int = 60 * 60,
    setback: int = 15 * 60,
    lead_gap: int = 4 * 60 * 60,
):
    """Generate labels csv mapping parquet files to integers.

    Requires converted parquet dataset to be split, see `split` and `convert`.

    Saves csv files mapping filenames to labels in PARQUET_PATH.

    Args:
        patient_id: patient id.
        forecast_window: size of forecast window in seconds.
        setback: Time in between forecast window and sztime in seconds (0 to disable setback).
        lead_gap: Size of window where following seizures are dropped (-1 to disable dropping).
    """

    assert str(patient_id) in PATIENT_IDS, f"{patient_id} not in {PATIENT_IDS}"
    assert all((PARQUET_PATH / str(patient_id) / split).exists() for split in SPLIT_NAMES), \
        "Not all splits exist, run `eadata split <pid> <train_prop> <test_prop>` first."

    with open(str(SZTIMES_PATH / f'{patient_id}.pkl'), 'rb') as f:
        sztimes = pickle.load(f)['utc']

    # drop non-lead seizures
    sztimes = sztimes[sztimes.diff().dt.total_seconds().fillna(1e6) > lead_gap]
    sztimes = sztimes.reset_index(drop=True)

    # Process sztimes into a series of UTC times (minute floored) which are positive labels
    positive_times = None
    for t in sztimes:
        forecast_times = t + pd.to_timedelta(np.arange(-forecast_window, 0, 60) - setback, 's')
        forecast_times = pd.Series(forecast_times).dt.floor('1min')
        positive_times = pd.concat([positive_times, forecast_times])

    # Create csv for each split
    for split_name in SPLIT_NAMES:
        logger.info(f'Creating labels csv for patient_id {patient_id} {split_name} split')
        split_labels_path = ARTIFACTS_PATH / f'{patient_id}_{split_name}_labels.csv'
        split_labels_path.parent.mkdir(exist_ok=True, parents=True)

        # Remove exising labels csv if present
        if split_labels_path.exists():
            split_labels_path.unlink()

        split_path = PARQUET_PATH / str(patient_id) / split_name
        split_files = sorted([
            Path().joinpath(*fp.parts[-4:])
            for fp in split_path.glob('**/*')
            if fp.suffix == '.parquet'
        ])

        labels_df = pd.DataFrame(split_files, columns=['filepath'])
        get_time = lambda x: pd.to_datetime(x.stem, format=TIMESTAMP_FORMAT, utc=True)
        labels_df['label'] = labels_df.filepath.apply(get_time).isin(positive_times).astype(int)

        if not labels_df.label.any():
            logger.warning(f"No labels for {patient_id} {split_name}")

        labels_df.to_csv(split_labels_path, index=False)
