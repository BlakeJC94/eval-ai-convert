from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from pytz import utc, timezone
from pytz import timezone, utc
import numpy as np
import mne
import warnings
import pandas as pd
from typing import List, Optional, Dict, Any

mne.set_log_level(False)

DATA_DIR = './edf'
OUTPUT_DIR = './parquet'

PATIENT_IDS = ['1110', '1869', '1876', '1904', '1965', '2002']
DTYPES = ['ACC', 'BVP', 'EDA', 'HR', 'TEMP']


def all_session_dirs(patient_id: str) -> List[Path]:
    """Get list of all Session dirs for a given patient.

    Args:
        patient_id: Patient ID.

    Returns:
        List of paths to session dirs.
    """
    assert patient_id in PATIENT_IDS, "invalid patient id"
    session_dirs = [
        p for p in Path(f'{DATA_DIR}/{patient_id}').glob('**/*')
        if p.is_dir() and not p.stem in ['training', 'testing']
    ]
    return session_dirs

def load_session_files(session_dir: Path) -> Dict[str, Any]:
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

def get_index_from_file(file: mne.io.edf.edf.RawEDF) -> pd.DatetimeIndex:
    """Get time index from file. First loads as US/Central time, then converted to UTC.

    Args:
        file: MNE RawEDF file.

    Returns:
        pandas DatatimeIndex generated from file start (UTC) and times vector from file.
    """
    start = file.info['meas_date'].replace(tzinfo=timezone('US/Central'))
    start = start.astimezone(utc)
    index = pd.to_datetime(start) + pd.to_timedelta(file.times, 's')
    return index

def get_channel_group_dataframe(file: mne.io.edf.edf.RawEDF, dtype: str) -> pd.DataFrame:
    """Converts file into dataframe.

    Args:
        file: MNE RawEDF file.

    Returns:
        Dataframe with columns corresponding to channels in file and UTC time index.
    """
    column_names = {
        'ACC': ['acc_x', 'acc_y', 'acc_z', 'acc_mag'],
        'BVP': ['bvp'],
        'EDA': ['eda'],
        'HR': ['hr'],
        'TEMP': ['temp'],
    }
    if file is None:
        file_df = pd.DataFrame(np.nan, index=df.index, columns=column_names[dtype])
    else:
        file_df = pd.DataFrame(
            file.get_data().transpose(),
            index=get_index_from_file(file),
            columns=column_names[dtype],
        )
    return file_df


def pad_session_df(df: pd.DataFrame) -> pd.DataFrame:
    """Pads start and end of dataframe with NaNs up to start of UTC minute and end of UTC minute.

    Args:
        df: session dataframe.

    Returns:
        Padded dataframe.
    """
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

    return df


def convert_session_to_dataframe(session_dir: Path, inner_join: bool = False) -> pd.DataFrame:
    """Converts a session directory into a pandas dataframe.

    Args:
        session_dir: Path to session directory.

    Returns:
        Dataframe with columns corresponding to data types (in order of DTYPES). If all the channel
        groups are bad, returns None.
    """
    files = load_session_files(session_dir)
    if all(f is None for f in files.values()):
        return None

    file_dfs = (get_channel_group_dataframe(files[dtype], dtype) for dtype in DTYPES)

    df = pd.concat(file_dfs, axis=1, join=('inner' if inner_join else 'outer'))
    return df

def save_session_to_parquet(df: pd.DataFrame, session_dir: Path) -> None:
    # split into chunks of 1 minute
    chunks = df.groupby(pd.Grouper(key='time',freq='1Min'))
    for t, chunk in chunks:
        table = pa.Table.from_pandas(chunk)

        pq_path = Path('pq').joinpath(*session_dir.parts[1:])
        pq_path.mkdir(parents=True, exist_ok=True)
        pq_path = pq_path / t.strftime('UTC-%Y_%m_%d-%H_%M_%S.parquet')

        pq.write_table(table, pq_path, version='2.6')  # version required for nanosecond timestamps

def write_dodgy_sessions(dodgy_sessions: List[Path], patient_id: str) -> None:
    if len(dodgy_sessions) == 0:
        print(f"  No dodgy sessions encountered.")
        return

    dodgy_list = Path('pq') / patient_id / 'dodgy_files.txt'
    with open(str(dodgy_list), 'w') as f:
        for session_dir in dodgy_sessions:
            f.write(str(session_dir) + '\n')


if __name__ == '__main__':
    patient_id = '1110'

    session_dirs = all_session_dirs(patient_id)
    session_dirs = sorted(session_dirs, key=lambda p: p.stem)

    dodgy_sessions = []
    for i, session_dir in enumerate(session_dirs, start=1):
        print(f"{i}/{len(session_dirs)} : {session_dir}")
        df = convert_session_to_dataframe(session_dir)
        if df is None:
            print(f"  WARNING : {session_dir} is dodgy, skipping")
            dodgy_sessions.append(session_dir)
            continue

        df = pad_session_df(df)
        df = df.reset_index().rename(columns={'index': 'time'})
        save_session_to_parquet(df, session_dir)

    write_dodgy_sessions(dodgy_sessions, patient_id)


