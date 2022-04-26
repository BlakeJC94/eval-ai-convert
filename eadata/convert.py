from pathlib import Path
import logging
import sys
import multiprocessing as mp
from typing import Optional

from tqdm import tqdm

from .data import (
    get_session_dataframe,
    save_session_to_parquet,
)
from .paths import (
    EDF_PATH,
    ARTIFACTS_PATH,
    all_session_dirs,
    write_dodgy_sessions,
)

logger = logging.getLogger(__name__)


def convert(patient_id: str, multiproc: bool = True) -> None:
    """Converts all sessions from EDF files to parquet files"""
    session_dirs = all_session_dirs(str(patient_id))

    if not multiproc:
        logger.info("Converting sessions using single process")
        dodgy_session_dirs = []
        for session_dir in tqdm(session_dirs, ncols=80):
            out = _convert_session(session_dir)
            dodgy_session_dirs.append(out)

    else:
        logger.info("Converting sessions using 6 parallel processes")
        with mp.Pool(6) as pool:
            dodgy_session_dirs = list(
                tqdm(
                    pool.imap(_convert_session, session_dirs),
                    desc='Converting sessions',
                    total=len(session_dirs),
                    file=sys.stdout,
                    ncols=80,
                ))

    dodgy_sessions = [i for i in dodgy_session_dirs if i is not None]
    write_dodgy_sessions(dodgy_sessions, patient_id)


def _convert_session(session_dir: Path) -> Optional[Path]:
    df = get_session_dataframe(session_dir)
    if df is None:
        logger.warning(f"{session_dir} is dodgy, skipping")
        return session_dir

    save_session_to_parquet(df, session_dir)
    return None
