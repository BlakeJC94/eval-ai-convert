"""Functions for saving parquet files."""
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from eadata.paths import PARQUET_PATH, all_session_dirs, get_session_ind


def save_session_to_parquet(df: pd.DataFrame, session_dir: Path) -> None:
    """Splits session df to 1 hour chunks and saves to Parquet following session_dir dirs.

    Args:
        df: dataframe of session.
        session_dir: Path of session directory.
    """
    pid, session_timestamp = session_dir.parts[-3], session_dir.parts[-1]
    session_ind = session_timestamp
    # TODO replace session_timestamp
    # session_ind = get_session_ind(pid, session_timestamp)
    pq_dir = Path(PARQUET_PATH) / pid / session_ind
    pq_dir.mkdir(parents=True, exist_ok=True)

    chunks = df.groupby(pd.Grouper(key='time', freq='H'))
    for time, chunk in chunks:
        chunk = chunk.drop('time', axis=1)  # Try droppping time to see if this makes files smaller.
        table = pa.Table.from_pandas(chunk)

        pq_path = pq_dir / time.strftime('UTC-%Y_%m_%d-%H_%M_%S.parquet')
        pq.write_table(table, pq_path)
