from .data import (
    all_session_dirs,
    get_session_dataframe,
    save_session_to_parquet,
)
from .paths import (
    EDF_PATH,
    ARTIFACTS_PATH,
    all_session_dirs,
    write_dodgy_sessions,
)


def convert(patient_id: str) -> None:
    """Converts all sessions from EDF files to parquet files"""
    session_dirs = all_session_dirs(patient_id)

    dodgy_sessions = []
    for i, session_dir in enumerate(session_dirs, start=1):
        print(f"{i}/{len(session_dirs)} : {session_dir}")

        df = get_session_dataframe(session_dir)
        if df is None:
            print(f"  WARNING : {session_dir} is dodgy, skipping")
            dodgy_sessions.append(session_dir)
            continue

        save_session_to_parquet(df, session_dir)

    write_dodgy_sessions(dodgy_sessions, patient_id)
