import shutil
import logging
from typing import List, Union

import numpy as np

from .globals import PATIENT_IDS, SPLIT_NAMES
from .paths import PARQUET_PATH

logger = logging.getLogger(__name__)


def split(patient_id: str, proportions: List[Union[float, int]]):
    """Create train/test split across sessions in parquet.

    Requires converted data in parquet format, see `convert`.

    Splits data in `./data/parquet/<pid>` by file size into `./data/parquet/<split>/<pid>`, where
    `<split>` is either `train`, or `test`. Existing splits will be undone before creating new
    split.

    Args:
        patient_id: Patient ID.
        proportions: Proportions of data to use for testing, remaining proportion for training.
    """
    assert all(p > 0 for p in proportions), "Expected valid `proportions`."
    assert len(proportions) == 2, "Expected 2 proportions for train/test split"
    assert str(patient_id) in PATIENT_IDS, "Patient ID not found."

    patient_path = PARQUET_PATH / str(patient_id)

    if not patient_path.exists() or not any(patient_path.iterdir()):
        raise FileNotFoundError('Convert data to parquet before running')

    # normalise proportions
    train_prop, test_prop = [p / sum(proportions) for p in proportions]
    logger.info(f"Creating train/test/val split of {train_prop} : {test_prop}")

    # If sessions in PARQUET_DIR are already split, undo the split before proceeding
    for split_name in SPLIT_NAMES:
        split_path = PARQUET_PATH / split_name / str(patient_id)
        if split_path.exists():
            logger.info(f"Found {split_name} split for {patient_id =}, undoing before proceeding")
            for session in split_path.iterdir():
                shutil.move(str(session), str(patient_path))
            shutil.rmtree(str(split_path))

    # Get all session dirs in PARQUET_DIR
    session_dirs = sorted(list(p for p in patient_path.glob('**/*') if not p.is_file()))

    # Get cumulative sums of session sizes
    dir_size = lambda d: np.sum(np.fromiter((f.stat().st_size for f in d.glob('*')), np.int64))
    session_sizes = np.fromiter((dir_size(d) for d in session_dirs), np.int64)
    c_props = np.cumsum(session_sizes) / np.sum(session_sizes)

    # Split session_dirs into splits of given proportions
    split_idx = np.where(c_props <= train_prop)[0][-1]
    splits = [session_dirs[:split_idx], session_dirs[split_idx:]]

    # Move sessions to their splits
    for split_name, session_dirs in zip(SPLIT_NAMES, splits):
        split_path = PARQUET_PATH / split_name / str(patient_id)
        split_path.mkdir(exist_ok=True, parents=True)
        logger.info(f"Creating {split_name} split")

        if len(session_dirs) == 0:
            logger.warning(f"No sessions in {split_name} split.")
            continue

        for session in session_dirs:
            shutil.move(str(session), str(split_path))

    # Remove empty directories
    if not any(patient_path.iterdir()):
        patient_path.rmdir()
