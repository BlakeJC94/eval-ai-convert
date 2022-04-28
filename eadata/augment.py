from pathlib import Path
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .globals import PATIENT_IDS, SPLIT_NAMES, TIMESTAMP_FORMAT, SRATE
from .paths import PARQUET_PATH, ARTIFACTS_PATH

logger = logging.getLogger(__name__)


def augment(
    patient_id: str,
    forecast_window: int = 60 * 60,
    setback: int = 15 * 60,
    step_size: int = 1 * 60,
):
    """Augments training split in converted data by creating a sliding window.

    Requires split converted data to be labeled. See `convert`, `split`, and `label`

    Args:
        patient_id: patient id.
        forecast_window: size of forecast window in seconds.
        setback: Time in between forecast window and sztime in seconds (0 to disable setback).
        step_size: Time to slide the window by (in seconds) in each step.
    """

    print("THESE REQUIREMENTS NEED MORE WORK")

    # split = SPLIT_NAMES[0]

    # assert str(patient_id) in PATIENT_IDS, f"{patient_id} not in {PATIENT_IDS}"
    # assert (PARQUET_PATH / str(patient_id) / split).exists(), \
    #     f"Train split doesn't, run `eadata split {patient_id} <train_prop> <test_prop>` first."

    # # Get training split files
    # training_files = [
    #     fp for fp in (PARQUET_PATH / str(patient_id) / split).glob('**/*')
    #     if fp.is_file() and fp.suffix == '.parquet'
    # ]
    # training_files = sorted(training_files, key=lambda fp: fp.stem)
    # times = [pd.to_datetime(fp.stem, format=TIMESTAMP_FORMAT) for fp in training_files]

    # # Load training labels csv and get each file with positive label
    # labels_path = ARTIFACTS_PATH / f'{patient_id}_{split}_labels.csv'
    # labels = pd.read_csv(labels_path)
    # positive_samples = labels.filepath[labels.label == 1].tolist()

    # for fp in positive_samples:
    #     sample = pd.read_parquet(PARQUET_PATH / fp)
    #     timestamp = pd.to_datetime(Path(fp).stem, format=TIMESTAMP_FORMAT)
    #     file_step = pd.to_timedelta(1, 'H')
    #     start_time = timestamp
    #     end_time = timestamp + file_step

    #     nearby = []
    #     for step in [-file_step, file_step]:
    #         other_timestamp = timestamp + step

    #         try:
    #             ind = times.index(other_timestamp)
    #             other_file = pd.read_parquet(PARQUET_PATH / training_files[ind])
    #             nearby.append(other_file)
    #             start_time = min(start_time, other_timestamp)
    #             end_time = max(end_time, other_timestamp + file_step)
    #         except ValueError:
    #             nearby.append(None)

    #     sample = pd.concat([nearby[0], sample, nearby[1]]).reset_index(drop=True)
    #     sample.index = pd.date_range(start_time, end_time, freq=f'{1/SRATE}S', inclusive='left')

    # # TODO for each positive file, load and join neighbouring files in dataframe
    # # * This will need to be limited by session edges!

    # # TODO slide window of size `forecasting_win` by step `augment_step` until reaching `setback`

    # # TODO Save each window as a new file to the training split

    # # TODO Add positive labels to training labels csv

    # # TODO Re-sort csv and save

    # pass
