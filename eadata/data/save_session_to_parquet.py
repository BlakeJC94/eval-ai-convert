"""Functions for saving parquet files."""
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from eadata.paths import PARQUET_PATH, all_session_dirs, get_session_ind


def save_session_to_parquet(
    df: pd.DataFrame,
    session_dir: Path,
    win_size: int = 1 * 60 * 60,
    win_step: int = 1 * 60 * 60,
) -> None:
    """Splits session df to 1 hour chunks and saves to Parquet following session_dir dirs.

    Args:
        df: dataframe of session.
        session_dir: Path of session directory.
        win_size: size of window in seconds.
        win_step: time to step by in seconds.
    """
    pid, session_timestamp = session_dir.parts[-3], session_dir.parts[-1]
    session_ind = get_session_ind(pid, session_timestamp)

    # Make directory for converted session of data
    pq_dir = Path(PARQUET_PATH) / pid / session_ind
    pq_dir.mkdir(parents=True, exist_ok=True)

    # Iterate over window starts and select window of data
    chunk_starts = pd.date_range(start=df.index[0], end=df.index[-1], freq=f"{win_step}S")
    for start in chunk_starts:
        chunk = df[(df.index >= start) & (df.index < start + pd.Timedelta(seconds=win_size))]

        # Try dropping time to see if this makes files smaller.
        chunk = chunk.reset_index().drop('index', axis=1)

        # Save chunk as parquet
        table = pa.Table.from_pandas(chunk)
        pq_path = pq_dir / start.strftime('UTC-%Y_%m_%d-%H_%M_%S.parquet')
        pq.write_table(table, pq_path)
