# Select a patient
# Get ordered list of each session directory
# Loop over each session directory
#    Start a block at the HH_MM_00 UTC timestamp
#        If a block already exists, load it and fill in new values
#    Fill NaN between block start and data start
#    Determine how many neat blocks can be created and create them
#    Last block, fill NaNs between data end and end of block
from pathlib import Path
from tqdm import tqdm
from pytz import utc, timezone
from datetime import timedelta as td
import numpy as np
import mne
import warnings

mne.set_log_level(False)

DATA_DIR = './edf'
OUTPUT_DIR = './parquet'

PATIENT_ID = '1904'
DTYPES = ['ACC', 'BVP', 'EDA', 'HR', 'TEMP']

# Get ordered list of each session directory
session_dirs = [
    p for p in Path(f'{DATA_DIR}/{PATIENT_ID}').glob('**/*')
    if p.is_dir() and not p.stem in ['training', 'testing']
]
session_dirs = sorted(session_dirs, key=lambda p: p.stem)

# session_dirs = [session_dirs[0]]
dodgy_sessions = []
for session_dir in tqdm(session_dirs, ncols=80):
    files = []
    session_is_dodgy = False
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        for dtype in DTYPES:
            try:
                files.append(mne.io.read_raw_edf(f"{session_dir}/Empatica-{dtype}.edf"))
            except:
                files.append(None)

    if all(f is None for f in files):
        dodgy_sessions.append(session_dir)
        continue

    start_times = [f.info['meas_date'] for f in files if f is not None]
    max_start_time, min_start_time = max(start_times), min(start_times)

    # send a warning if there's a large gap between start times in each channel
    start_gap = (max_start_time - min_start_time).total_seconds()
    if start_gap > 5:
        print( f"\nThere's a range of {start_gap} seconds in start times for {session_dir.stem}")

    end_times = [f.info['meas_date'] + td(seconds=f.times[-1]) for f in files if f is not None]
    max_end_time, min_end_time = max(end_times), min(end_times)

    # send a warning if there's a large gap between end times in each channel
    end_gap = (max_end_time - min_end_time).total_seconds()
    if end_gap > 5:
        print( f"\nThere's a range of {end_gap} seconds in end times for {session_dir.stem}")

    # trim the data indices based on the time vectors
    start_inds, end_inds = [], []
    for f in files:
        if f is not None:
            file_start_time = f.info['meas_date']
            start_trim = (max_start_time - file_start_time).total_seconds()
            try:
                start_ind = np.where(f.times >= start_trim)[0][0]
            except:
                breakpoint()
            start_inds.append(start_ind)

            file_end_time = f.info['meas_date'] + timedelta(seconds=f.times[-1])
            end_trim = (file_end_time - min_end_time).total_seconds()
            end_ind = np.where(f.times <= f.times[-1] - end_trim)[0][-1]
            end_inds.append(end_ind)
        else:
            start_inds.append(None)
            end_inds.append(None)

    lengths = [len(f.times[s:1+e]) for f,s,e in zip(files, start_inds, end_inds) if f is not None]
    assert len(set(lengths)) == 1



    [(f.info['meas_date'] - max_start_time).total_seconds() for f in files]
    [np.argwhere(f.times >= (f.info['meas_date'] - max_start_time).total_seconds()) for f in files]
    ...

    start_time = max_start_time
    start_time = start_time.replace(tzinfo=timezone('US/Cental'))
    start_time_utc = start_time.astimezone(utc)

    end_time = min_end_time
    end_time = end_time.replace(tzinfo=timezone('US/Cental'))
    end_time_utc = end_time.astimezone(utc)

    n_blocks = np.ceil((end_time_utc - start_time_utc).total_seconds() / 60)
    assert n_blocks > 3


    # Create first block
    block_path = start_time_utc.strftime('Data_%Y_%m_%d/Hour_%H/UTC_%H_%M_%S.pq')


