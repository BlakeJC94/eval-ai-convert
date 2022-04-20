from pathlib import Path
from tqdm import tqdm
from pytz import utc, timezone
from datetime import timedelta as td
import numpy as np
import mne
import warnings
import pandas as pd

mne.set_log_level(False)

DATA_DIR = './edf'
OUTPUT_DIR = './parquet'

PATIENT_ID = '1876'
DTYPES = ['ACC', 'BVP', 'EDA', 'HR', 'TEMP']

# Get ordered list of each session directory
session_dirs = [
    p for p in Path(f'{DATA_DIR}/{PATIENT_ID}').glob('**/*')
    if p.is_dir() and not p.stem in ['training', 'testing']
]

# remove dodgy sessions
print("FILTERING")
dodgy_sessions = []
for session_dir in tqdm(session_dirs, ncols=80):
    files = []
    for dtype in DTYPES:
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            try:
                files.append(mne.io.read_raw_edf(f"{session_dir}/Empatica-{dtype}.edf"))
            except:
                files.append(None)

    if all(f is None for f in files):
        dodgy_sessions.append(session_dir)

session_dirs = [p for p in session_dirs if p not in dodgy_sessions]
session_dirs = sorted(session_dirs, key=lambda p: p.stem)

# session_dirs = [session_dirs[0]]
all_stats = []
for session_dir in tqdm(session_dirs, ncols=80):
    files = []
    for dtype in DTYPES:
        try:
            files.append(mne.io.read_raw_edf(f"{session_dir}/Empatica-{dtype}.edf"))
        except:
            files.append(None)

    start_times = [f.info['meas_date'] for f in files if f is not None]
    max_start_time, min_start_time = max(start_times), min(start_times)

    end_times = [f.info['meas_date'] + td(seconds=f.times[-1]) for f in files if f is not None]
    max_end_time, min_end_time = max(end_times), min(end_times)

    stats = {
        'session_dir': session_dir,
        'min_start_time': min_start_time,
        'max_end_time': max_end_time,
    }
    all_stats.append(stats)

times_df = pd.DataFrame(all_stats)


