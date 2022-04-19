from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def save_session_to_parquet(df: pd.DataFrame, session_dir: Path) -> None:
    """Splits session df to 1 minute chunks and saves to Parquet following session_dir dirs.

    Args:
        df: dataframe of session.
        session_dir: Path of session directory.
    """
    chunks = df.groupby(pd.Grouper(key='time',freq='1Min'))
    for t, chunk in chunks:
        table = pa.Table.from_pandas(chunk)

        pq_path = Path('parquet').joinpath(*session_dir.parts[1:])
        pq_path.mkdir(parents=True, exist_ok=True)
        pq_path = pq_path / t.strftime('UTC-%Y_%m_%d-%H_%M_%S.parquet')

        pq.write_table(table, pq_path, version='2.6')  # version required for nanosecond timestamps
