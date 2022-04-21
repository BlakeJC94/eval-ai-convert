import sys
import logging
import multiprocessing as mp
from typing import List
from pathlib import Path
import warnings

from tqdm import tqdm
import mne
import pandas as pd

from eadata.paths import EDF_PATH

mne.set_log_level(False)
logger = logging.getLogger(__name__)

def _get_start(fp):
    tz = None
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        try:
            f = mne.io.read_raw_edf(str(fp))
            return f.info['meas_date'].replace(tzinfo=tz)
        except:
            return None


def get_file_start_times(patient_ids: List[str], multiproc: bool = False):
    edf_files = []
    for pid in patient_ids:
        files_glob = (EDF_PATH / pid).glob('**/*')
        edf_files.extend([i for i in files_glob if i.suffix == '.edf' and i.name[0] != '.'])

    if not multiproc:
        logger.info("Getting file start times using single process")
        starts = []
        for fp in tqdm(edf_files, ncols=80):
            starts.append(_get_start(fp))
    else:
        logger.info(f"Getting file start times using parallel processes")
        with mp.Pool(6) as pool:
            starts = list(
                tqdm(
                    pool.imap(_get_start, edf_files, chunksize=50),
                    desc='Getting file start times',
                    total=len(edf_files),
                    file=sys.stdout,
                    ncols=80,
                ))

    df = pd.DataFrame.from_dict({'filepath': edf_files, 'start': starts})
    return df
