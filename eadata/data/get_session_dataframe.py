import warnings
from pathlib import Path
from pytz import timezone, utc
from typing import Optional, Dict

import pandas as pd
import numpy as np
import mne

from eadata.globals import DTYPES, SRATE

mne.set_log_level(False)


def get_session_dataframe(
    session_dir: Path,
    inner_join: bool = False,
    pad: bool = True,
) -> Optional[pd.DataFrame]:
    """Converts a session directory into a pandas dataframe.

    Args:
        session_dir: Path to session directory.

    Returns:
        Dataframe with columns corresponding to data types (in order of DTYPES). If all the channel
        groups are bad, returns None.
    """
    files = load_session_data(session_dir)
    if all(f is None for f in files.values()):
        return None

    file_dfs = (_get_channel_group_dataframe(files[dtype], dtype) for dtype in DTYPES)
    df = pd.concat(file_dfs, axis=1, join=('inner' if inner_join else 'outer'))

    if pad:
        df = _pad_session_df(df)

    return df


def _pad_session_df(df: pd.DataFrame) -> pd.DataFrame:
    """Pads start and end of dataframe with NaNs up to start of UTC minute and end of UTC minute.

    Args:
        df: session dataframe.

    Returns:
        Padded dataframe.
    """
    df_start = df.index[0]
    block_start = df_start.floor('H')
    if block_start != df_start:
        start_pad_index = pd.to_timedelta(
            np.arange((df_start - block_start).total_seconds(), 0, -1 / 128), 's')
        start_pad = pd.DataFrame(np.nan, index=df_start - start_pad_index, columns=df.columns)
        df = pd.concat([start_pad, df], axis=0)

    df_end = df.index[-1]
    block_end = df_end.ceil('H')
    if block_end != df_end:
        end_pad_index = pd.to_timedelta(
            np.arange(1 / 128, (block_end - df_end).total_seconds(), 1 / 128), 's')
        end_pad = pd.DataFrame(np.nan, index=df_end + end_pad_index, columns=df.columns)
        df = pd.concat([df, end_pad], axis=0)

    assert (block_end - block_start).total_seconds() * SRATE == len(df), \
        "FATAL :Incorrect padding or join operation occured."
    return df


def _get_index_from_file(file: mne.io.edf.edf.RawEDF) -> pd.DatetimeIndex:
    """Get time index from file. First loads as US/Central time, then converted to UTC.

    Args:
        file: MNE RawEDF file.

    Returns:
        pandas DatatimeIndex generated from file start (UTC) and times vector from file.
    """
    tz = timezone('US/Central')
    start = file.info['meas_date'].replace(tzinfo=tz)
    start = start.astimezone(utc)
    index = pd.to_datetime(start) + pd.to_timedelta(file.times, 's')
    return index


def _get_channel_group_dataframe(
    file: mne.io.edf.edf.RawEDF,
    dtype: str,
) -> pd.DataFrame:
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
        file_df = pd.DataFrame(columns=column_names[dtype])
    else:
        file_df = pd.DataFrame(
            data=np.array(file.get_data().transpose(), dtype=np.float32),
            index=_get_index_from_file(file),
            columns=column_names[dtype],
        )
    return file_df


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
