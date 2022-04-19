from pathlib import Path
from typing import List, Dict, Optional
import warnings

import mne

from eval_ai_convert.globals import PATIENT_IDS, DTYPES

mne.set_log_level(False)

def write_dodgy_sessions(dodgy_sessions: List[Path], patient_id: str) -> None:
    """Records dodgy files to txt file.

    Args:
        dodgy_sessions: List of session dirs.
        patient_id: Patient ID.
    """
    if len(dodgy_sessions) == 0:
        print(f"  No dodgy sessions encountered.")
        return

    dodgy_list = Path('pq') / patient_id / 'dodgy_files.txt'
    with open(str(dodgy_list), 'w') as f:
        for session_dir in dodgy_sessions:
            f.write(str(session_dir) + '\n')

def all_session_dirs(patient_id: str) -> List[Path]:
    """Get list of all Session dirs for a given patient.

    Args:
        patient_id: Patient ID.

    Returns:
        List of paths to session dirs.
    """
    assert patient_id in PATIENT_IDS, "invalid patient id"
    data_dir = './edf'
    session_dirs = [
        p for p in Path(f'{data_dir}/{patient_id}').glob('**/*')
        if p.is_dir() and not p.stem in ['training', 'testing']
    ]
    session_dirs = sorted(session_dirs, key=lambda p: p.stem)
    return session_dirs

def load_session_files(session_dir: Path) -> Dict[str, Optional[mne.io.edf.edf.RawEDF]]:
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

