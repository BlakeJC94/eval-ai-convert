import shutil
import logging

import numpy as np

from .globals import PATIENT_IDS, SPLIT_NAMES
from .paths import PARQUET_PATH

logger = logging.getLogger(__name__)


def split(patient_id: str, train_prop: float, test_prop: float):
    """Create train/test/val split across sessions in parquet.

    Requires converted data in parquet format, see `convert`.

    Splits data in `./data/parquet/<pid>` by file size into `./data/parquet/<pid>/<split>`, where
    `<split>` is either `train`, `test`, or `val`. Existing splits will be undone before creating
    new split.

    Args:
        patient_id: Patient ID.
        train_prop: Proportion of data to use for training.
        test_prop: Proportion of data to use for testing, remaining proportion for validation.
    """
    assert train_prop > 0 and test_prop > 0
    assert train_prop + test_prop <= 1.0, "Proportions must sum to 1.0"
    assert str(patient_id) in PATIENT_IDS, "Patient ID not found"

    patient_path = PARQUET_PATH / str(patient_id)

    if not patient_path.exists() or not any(patient_path.iterdir()):
        raise FileNotFoundError('Convert data to parquet before running')

    val_prop = 1 - (train_prop + test_prop)
    logger.info(f"Creating train/test/val split of {train_prop} : {test_prop} : {val_prop}")

    # If sessions in PARQUET_DIR are already split, undo the split before proceeding
    for split_name in SPLIT_NAMES:
        split_path = PARQUET_PATH / split_name / str(patient_id)
        if split_path.exists():
            logger.info(f"Found {split_name} split, undoing before proceeding")
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
    train_idx = np.where(c_props <= train_prop)[0][-1]
    test_idx = np.where((c_props > train_prop) & (c_props <= train_prop + test_prop))[0][-1]
    splits = [session_dirs[:train_idx], session_dirs[train_idx:test_idx], session_dirs[test_idx:]]
    if test_idx < len(session_dirs):
        splits.append(session_dirs[test_idx:])
    else:
        splits.append([])
    splits = dict((name, dirs) for name, dirs in zip(SPLIT_NAMES, splits))

    # Move sessions to their splits
    for split_name, session_dirs in splits.items():
        split_path = PARQUET_PATH / split_name / str(patient_id)
        split_path.mkdir(exist_ok=True)
        logger.info(f"Creating {split_name} split")

        if len(session_dirs) == 0:
            logger.warning(f"No sessions in {split_name} split.")
            continue

        for session in session_dirs:
            shutil.move(str(session), str(split_path))
