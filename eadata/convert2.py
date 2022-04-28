from pathlib import Path
import logging
import sys
import multiprocessing as mp
from typing import Optional

from tqdm import tqdm

from .data import (
    get_session_dataframe,
    save_session_to_parquet2,
)
from .paths import (
    EDF_PATH,
    ARTIFACTS_PATH,
    all_session_dirs,
    write_dodgy_sessions,
)

logger = logging.getLogger(__name__)


def convert2(patient_id: str, multiproc: bool = True) -> None:
    """Converts all sessions from EDF files to parquet files.

    Converts EDF files in `edf/<patient_id>/<session_timestamp>/*.edf` to Parquet files in
    `parquet/<patient_id>/<session_timestamp>/*.parquet`. EDF files are saved per channel group,
    whereas parquet files are saved per block of time.

    Some sessions may be dodgy, in which case they are skipped and recorded to artifacts.

    Args:
        patient_id: Patient ID to convert.
        multiproc: Whether to use multiprocessing
    """
    session_dirs = all_session_dirs(str(patient_id))

    if not multiproc:
        logger.info("Converting sessions using single process")
        dodgy_session_dirs = []
        for session_dir in tqdm(session_dirs, ncols=80):
            out = _convert_session(session_dir)
            dodgy_session_dirs.append(out)

    else:
        logger.info("Converting sessions using parallel processes")
        with mp.Pool() as pool:
            dodgy_session_dirs = list(
                tqdm(
                    pool.imap(_convert_session, session_dirs),
                    desc='Converting sessions',
                    total=len(session_dirs),
                    file=sys.stdout,
                    ncols=80,
                ))

    dodgy_sessions = [i for i in dodgy_session_dirs if i is not None]

    if len(dodgy_sessions) > 0:
        logger.warning(f"{len(dodgy_sessions)} sessions are dodgy, skipping")
        write_dodgy_sessions(dodgy_sessions, patient_id)


def _convert_session(session_dir: Path) -> Optional[Path]:
    """Helper function for multiprocessing.

    Args:
        session_dir: Path to session directory.

    Returns:
        None if successful. If unsucessful, returns session_dir"""
    df = get_session_dataframe(session_dir)
    if df is None:
        return session_dir

    save_session_to_parquet2(df, session_dir)
    return None
