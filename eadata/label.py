from pathlib import Path
import pickle
import logging

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from .globals import PATIENT_IDS, SPLIT_NAMES, TIMESTAMP_FORMAT, SRATE
from .paths import PARQUET_PATH, SZTIMES_PATH, ARTIFACTS_PATH

logger = logging.getLogger(__name__)


def label(
    patient_id: str,
    forecast_window: int = 60 * 60,
    setback: int = 15 * 60,
    lead_gap: int = 4 * 60 * 60,
):
    """Generate labels csv mapping parquet files to integers and augments dataset.

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
    window = pd.timedelta_range(pd.Timedelta(seconds=setback), 0, freq='-60s', closed='right')
    # Get times where seizure occurs in next `setback` seconds
    for t in sztimes:
        forecast_times = t.floor('1min') - window
        # Get corresponding parquet file times
        forecast_times = forecast_times - pd.Timedelta(seconds=forecast_window)
        # Add to list of positive times
        positive_times = pd.concat([positive_times, pd.Series(forecast_times)])

    all_files = sorted(
        [fp for fp in (PARQUET_PATH / str(patient_id)).glob('**/*') if fp.suffix == '.parquet'],
        key=lambda fp: fp.stem,
    )
    # Create csv for each split
    for split_name in SPLIT_NAMES:
        logger.info(f'Creating labels csv for patient_id {patient_id} {split_name} split')
        split_labels_path = ARTIFACTS_PATH / f'{patient_id}_{split_name}_labels.csv'
        split_labels_path.parent.mkdir(exist_ok=True, parents=True)

        # Remove exising labels csv if present
        if split_labels_path.exists():
            split_labels_path.unlink()

        # Get filenames in split
        split_files = [
            Path('data') / Path().joinpath(*fp.parts[-4:])
            for fp in all_files
            if fp.parent.parent.stem == split_name
        ]

        # Get list of times in split
        split_times = [
            pd.to_datetime(fp.stem, format=TIMESTAMP_FORMAT, utc=True) for fp in split_files
        ]

        # Create augmented samples for `positive_times` that are not in `split_times`
        split_mask = positive_times.dt.floor('H').isin(split_times)
        logger.info(f"Creating augmented samples for {split_name} split")
        for t in tqdm(positive_times[split_mask & ~positive_times.isin(split_times)]):
            timestamp = t.floor('H').strftime(TIMESTAMP_FORMAT)

            # Skip timestamp if there's no files that match request
            filepath_matches = [fp for fp in all_files if fp.stem == timestamp]
            if len(filepath_matches) == 0:
                continue

            # Get first file that matches timestamp
            filepath = sorted(filepath_matches)[0]
            session_dir = filepath.parent

            # skip timestamp if next file is not in the same session
            next_filepath = session_dir / f"{t.ceil('H').strftime(TIMESTAMP_FORMAT)}.parquet"
            if not next_filepath.exists():
                continue

            # load dataframes and create time index
            df = pd.concat([pd.read_parquet(filepath), pd.read_parquet(next_filepath)])
            df.index = pd.date_range(
                t.floor('H'),
                t.floor('H') + pd.Timedelta(hours=2),
                freq=f'{1/SRATE}S',
                inclusive='left',
            )

            # trim to requested hour window and save
            df = df[(df.index >= t) & (df.index < t + pd.Timedelta(hours=1))]
            df = df.reset_index().drop('index', axis=1)
            table = pa.Table.from_pandas(df)
            pq_path = session_dir / f"{t.strftime(TIMESTAMP_FORMAT)}.parquet"
            pq.write_table(table, pq_path)

            # Add new samples to split_files
            split_files.append(Path('data') / Path().joinpath(*pq_path.parts[-4:]))

        # Get label for each file arranged in a dataframe
        labels_df = pd.DataFrame(sorted(split_files), columns=['filepath'])
        get_time = lambda x: pd.to_datetime(x.stem, format=TIMESTAMP_FORMAT, utc=True)
        labels_df['label'] = labels_df.filepath.apply(get_time)
        labels_df['label'] = labels_df['label'].isin(positive_times)
        labels_df['label'] = labels_df['label'].astype(int)

        if not labels_df.label.any():
            logger.warning(f"No labels for {patient_id} {split_name}")

        labels_df.to_csv(split_labels_path, index=False)
