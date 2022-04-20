
from itertools import combinations
import warnings
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import mne
from tqdm import tqdm


DATA_DIR = './edf'
PATIENT_ID = '1110'
DTYPES = ['ACC', 'BVP', 'EDA', 'HR', 'TEMP']

mne.set_log_level(False)

session_dirs = [
    p for p in Path(f'{DATA_DIR}/{PATIENT_ID}').glob('**/*')
    if p.is_dir() and not p.stem in ['training', 'testing']
]
session_dirs = sorted(session_dirs, key=lambda p: p.stem)

dodgy_sessions = []
overhanging = []
all_unequal_durations, all_unequal_starts, all_unequal_ends = [], [], []
all_stats = []
for session_dir in tqdm(session_dirs, ncols=80):
    session_is_dodgy = False

    times, starts, ends = [], [], []
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        for dtype in DTYPES:
            try:
                file = mne.io.read_raw_edf(f"{session_dir}/Empatica-{dtype}.edf")
                times.append(file.times)
                starts.append(file.info['meas_date'])
                ends.append(file.info['meas_date'] + timedelta(seconds=file.times[-1]))
            except:
                session_is_dodgy = True
                times.append(None)
                starts.append(None)
                ends.append(None)

    if session_is_dodgy:
        dodgy_sessions.append(session_dir)

    # # Get pairs of channels that have unequal time vectors
    # unequal_times = [
    #     {
    #         'session_dir': session_dir,
    #         'ch_pair': f"{i}, {j}",
    #         'time_diff': np.abs(t2.max() - t1.max())
    #     }
    #     for (i, t1), (j, t2) in combinations(zip(DTYPES, times), 2)
    #     if not np.array_equal(t1, t2) and t1 is not None and t2 is not None
    # ]
    # times_max, times_min = None, None
    # if unequal_times:
    #     all_unequal_times.extend(unequal_times)
    #     times_max = max(i['time_diff'] for i in unequal_times)
    #     times_min = min(i['time_diff'] for i in unequal_times)


    # Get pairs of channels that have unequal durations
    unequal_durations = [
        {
            'session_dir': session_dir,
            'ch_pair': f"{i}, {j}",
            'duration_diff': np.abs(len(t2) - len(t1)),
        }
        for (i, t1), (j, t2) in combinations(zip(DTYPES, times), 2)
        if t1 is not None and t2 is not None and len(t1) != len(t2)
    ]
    duration_max, duration_min = None, None
    if unequal_durations:
        all_unequal_durations.extend(unequal_durations)
        duration_max = max(i['duration_diff'] for i in unequal_durations)
        duration_min = min(i['duration_diff'] for i in unequal_durations)

    # Get pairs of channels that have unequal start times
    unequal_starts = [
        {
            'session_dir': session_dir,
            'ch_pair': f"{i}, {j}",
            'start_diff': (t1 - t2).total_seconds()
        }
        for (i, t1), (j, t2) in combinations(zip(DTYPES, starts), 2)
        if not t1 == None and not t2 == None and t1 != t2
    ]
    starts_max, starts_min = None, None
    if unequal_starts:
        all_unequal_starts.extend(unequal_starts)
        starts_max = max(i['start_diff'] for i in unequal_starts)
        starts_min = min(i['start_diff'] for i in unequal_starts)

    # Get pairs of channels that have unequal end times
    unequal_ends = [
        {
            'session_dir': session_dir,
            'ch_pair': f"{i}, {j}",
            'end_diff': (t1 - t2).total_seconds()
        }
        for (i, t1), (j, t2) in combinations(zip(DTYPES, ends), 2)
        if not t1 == None and not t2 == None and t1 != t2
    ]
    ends_max, ends_min = None, None
    if unequal_ends:
        all_unequal_ends.extend(unequal_ends)
        ends_max = max(i['end_diff'] for i in unequal_ends)
        ends_min = min(i['end_diff'] for i in unequal_ends)

    stats = {
        'session_dir': session_dir,
        'starts_min': starts_min,
        'starts_max': starts_max,
        'ends_min': ends_min,
        'ends_max': ends_max,
        'duration_min': duration_min,
        'duration_max': duration_max,
    }
    all_stats.append(stats)

    if len(unequal_durations) + len(unequal_starts) > 0:
        overhanging.append(session_dir)

unequal_ends_df = pd.DataFrame(all_unequal_ends)
unequal_starts_df = pd.DataFrame(all_unequal_starts)
stats_df = pd.DataFrame(all_stats)

