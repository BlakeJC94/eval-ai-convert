import logging
from pathlib import Path
from typing import List

from .globals import PATIENT_IDS

SRC_DIR = Path(__file__).absolute().parent
ROOT_DIR = SRC_DIR.parent

DATA_DIR = ROOT_DIR / 'data'

EDF_PATH = DATA_DIR / 'edf'
PARQUET_PATH = DATA_DIR / 'parquet'
SZTIMES_PATH = DATA_DIR / 'sztimes'
ARTIFACTS_PATH = DATA_DIR / 'artifacts'
OUTPUT_DIR = DATA_DIR / 'output'

logger = logging.getLogger(__name__)


def all_session_dirs(patient_id: str) -> List[Path]:
    """Get list of all Session dirs for a given patient.

    Args:
        patient_id: Patient ID.

    Returns:
        List of paths to session dirs.
    """
    assert patient_id in PATIENT_IDS, "invalid patient id"
    session_dirs = [
        p for p in Path(f'{EDF_PATH}/{patient_id}').glob('**/*')
        if p.is_dir() and not p.stem in ['training', 'testing']
    ]
    session_dirs = sorted(session_dirs, key=lambda p: p.stem)
    return session_dirs


def get_session_ind(patient_id: str, session_timestamp: str) -> str:
    session_ind = sorted([fp.stem for fp in all_session_dirs(patient_id)]).index(session_timestamp)
    return str(session_ind).zfill(3)


def write_dodgy_sessions(dodgy_sessions: List[Path], patient_id: str) -> None:
    """Records dodgy files to txt file.

    Args:
        dodgy_sessions: List of session dirs.
        patient_id: Patient ID.
    """
    if len(dodgy_sessions) == 0:
        logger.info(f"No dodgy sessions encountered for {patient_id =}.")
        return

    dodgy_list_path = Path(ARTIFACTS_PATH) / str(patient_id)
    dodgy_list_path.mkdir(exist_ok=True, parents=True)

    dodgy_list = dodgy_list_path / 'dodgy_files.txt'
    with open(str(dodgy_list), 'w') as f:
        for session_dir in dodgy_sessions:
            f.write(str(session_dir) + '\n')
