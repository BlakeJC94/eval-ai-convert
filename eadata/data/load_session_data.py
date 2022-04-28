"""Functions for loading and processing edf files."""
from pathlib import Path
from typing import Dict, Optional
import warnings

import mne

from eadata.globals import DTYPES

mne.set_log_level(False)


def load_session_data(session_dir: Path) -> Dict[str, Optional[mne.io.edf.edf.RawEDF]]:
    """Loads each DTYPE edf file located in a session dir.

    Args:
        session_dir: Path to session directory.

    Returns:
        Dictionary of DTYPE files. If an error occurs during loading an EDF file, `None` is stored
        as the value.
    """
    files = {}
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        for dtype in DTYPES:
            fp = session_dir / f"Empatica-{dtype}.edf"
            try:
                files[dtype] = mne.io.read_raw_edf(fp)
            except:
                files[dtype] = None

    return files

