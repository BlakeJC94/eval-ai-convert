from pathlib import Path
from tqdm import tqdm
from pytz import utc, timezone
from datetime import timedelta
from pytz import timezone, utc
import numpy as np
import mne
import warnings
import pandas as pd

mne.set_log_level(False)

DATA_DIR = './edf'
OUTPUT_DIR = './parquet'

PATIENT_IDS = ['1110', '1869', '1876', '1904', '1965', '2002']
DTYPES = ['ACC', 'BVP', 'EDA', 'HR', 'TEMP']
FREQ = 128
COLUMN_NAMES = {
    'ACC': ['acc_x', 'acc_y', 'acc_z', 'acc_mag'],
    'BVP': ['bvp'],
    'EDA': ['eda'],
    'HR': ['hr'],
    'TEMP': ['temp'],
}


def all_session_dirs(patient_id):
    assert patient_id in PATIENT_IDS, "invalid patient id"
    session_dirs = [
        p for p in Path(f'{DATA_DIR}/{patient_id}').glob('**/*')
        if p.is_dir() and not p.stem in ['training', 'testing']
    ]
    return session_dirs

def load_session_files(session_dir):
    files = {}
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        for dtype in DTYPES:
            fp = f"{session_dir}/Empatica-{dtype}.edf"
            try:
                files[dtype] = mne.io.read_raw_edf(fp)
            except:
                files[dtype] = None

    return files

def get_index_from_file(file):
    start = file.info['meas_date'].replace(tzinfo=timezone('US/Central'))
    start = start.astimezone(utc)
    index = pd.to_datetime(start) + pd.to_timedelta(file.times, 's')
    return index

def get_channel_group_dataframe(file, dtype):
    if file is None:
        file_df = pd.DataFrame(np.nan, index=df.index, columns=COLUMN_NAMES[dtype])
    else:
        file_df = pd.DataFrame(
            file.get_data().transpose(),
            index=get_index_from_file(file),
            columns=COLUMN_NAMES[dtype],
        )
    return file_df

def convert_session_to_dataframe(session_dir, inner_join=False):
    files = load_session_files(session_dir)

    if all(f is None for f in files.values()):
        return None

    file_dfs = (get_channel_group_dataframe(file, dtype) for dtype, file in files.items())
    df = pd.concat(file_dfs, axis=1, join=('inner' if inner_join else 'outer'))

    # Pad to first and last minute across all channels with NaNs
    df_start = df.index[0]
    block_start = df_start.floor('1min')
    if block_start != df_start:
        start_pad_index = pd.to_timedelta(np.arange((df_start - block_start).total_seconds(), 0, -1/128), 's')
        start_pad = pd.DataFrame(np.nan, index=df_start - start_pad_index, columns=df.columns)
        df = pd.concat([start_pad, df], axis=0)

    df_end = df.index[-1]
    block_end = df_end.ceil('1min')
    end_pad = pd.DataFrame()
    if block_end != df_end:
        end_pad_index = pd.to_timedelta(np.arange(1/128, (block_end - df_end).total_seconds(), 1/128), 's')
        end_pad = pd.DataFrame(np.nan, index=df_end + end_pad_index, columns=df.columns)
        df = pd.concat([df, end_pad], axis=0)

    df = df.reset_index().rename(columns={'index': 'time'})
    return df


if __name__ == '__main__':
    patient_id = '1110'

    session_dirs = all_session_dirs(patient_id)

    # TEST
    session_dirs = session_dirs[:1]

    dodgy_sessions = []
    for session_dir in tqdm(session_dirs, ncols=80):
        df = convert_session_to_dataframe(session_dir)
        if df is None:
            dodgy_sessions.append(session_dir)
        else:
            pass
            # TODO split up into minutes and save to parquet


