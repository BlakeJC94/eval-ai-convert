from pathlib import Path
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .globals import PATIENT_IDS, SPLIT_NAMES, TIMESTAMP_FORMAT, SRATE
from .paths import PARQUET_PATH, ARTIFACTS_PATH

logger = logging.getLogger(__name__)


def clean(
    patient_id: str,
    forecast_window: int = 60 * 60,
    setback: int = 15 * 60,
    step_size: int = 1 * 60,
):
    """Removes augmented samples from splits and labels csvs.

    Requires split converted data to be labeled. See `convert`, `split`, and `label`

    Args:
        patient_id: patient id.
    """

    assert str(patient_id) in PATIENT_IDS, f"{patient_id} not in {PATIENT_IDS}"
    assert all((PARQUET_PATH / str(patient_id) / split).exists() for split in SPLIT_NAMES), \
        "Not all splits exist, run `eadata split <pid> <train_prop> <test_prop>` first."
    assert all((ARTIFACTS_PATH / f'{patient_id}_{split}_labels.csv').exists() for split in SPLIT_NAMES), \
        "Labels don't exist, run `eadata label <pid>` first."

    all_files = sorted(
        [fp for fp in (PARQUET_PATH / str(patient_id)).glob('**/*') if fp.suffix == '.parquet'],
        key=lambda fp: fp.stem,
    )
    for split_name in SPLIT_NAMES:
        split_files = [fp for fp in all_files if fp.parent.parent.stem == split_name]
        files_to_delete = [
            fp for fp in split_files
            if pd.to_datetime(fp.stem, format=TIMESTAMP_FORMAT).minute != 0
        ]

        for fp in files_to_delete:
            fp.unlink()

        files_to_delete = [str(Path().joinpath(*fp.parts[-4:])) for fp in files_to_delete]

        split_labels_path = ARTIFACTS_PATH / f'{patient_id}_{split_name}_labels.csv'
        labels_df = pd.read_csv(split_labels_path)

        labels_df = labels_df[~labels_df.filepath.isin(files_to_delete)].reset_index(drop=True)
        labels_df.to_csv(split_labels_path, index=False)


