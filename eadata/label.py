from pathlib import Path
import pickle
import logging

import pandas as pd
import numpy as np

from .globals import PATIENT_IDS, SPLIT_NAMES
from .paths import PARQUET_PATH, SZTIMES_PATH, ARTIFACTS_PATH

logger = logging.getLogger(__name__)


def label(
    patient_id: str,
    forecast_window: int = 60 * 60,
    setback: int = 15 * 60,
    lead_gap: int = 4 * 60 * 60,
):

    assert str(patient_id) in PATIENT_IDS, f"{patient_id} not in {PATIENT_IDS}"
    assert all((PARQUET_PATH / str(patient_id) / split).exists() for split in SPLIT_NAMES), \
        "Not all splits exist, run `eadata split <pid> <train_prop> <test_prop>` first."

    with open(str(SZTIMES_PATH / f'{patient_id}.pkl'), 'rb') as f:
        sztimes = pickle.load(f)['utc']

    # drop non-lead seizures
    sztimes = sztimes[sztimes.diff().dt.total_seconds() > lead_gap]

    # Process sztimes into a series of UTC times (minute floored) which are positive labels
    positive_times = pd.Series(dtype=int)
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

        split_labels = pd.DataFrame(split_files, columns=['filepath'])
        get_time = lambda x: pd.to_datetime(x.stem, format='UTC-%Y_%m_%d-%H_%M_%S', utc=True)
        split_labels['label'] = split_labels.filepath.apply(get_time).isin(positive_times)

        if not split_labels.label.any():
            logger.warning(f"No labels for {patient_id} {split_name}")

        split_labels.to_csv(split_labels_path, index=False)
