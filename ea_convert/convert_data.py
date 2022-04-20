import eval_ai_convert.cvdata as cvdata
from .globals import PATIENT_IDS

def convert_data(patient_id: str) -> None:
    """Converts all sessions from EDF files to parquet files"""
    session_dirs = cvdata.all_session_dirs(patient_id)

    dodgy_sessions = []
    for i, session_dir in enumerate(session_dirs, start=1):
        print(f"{i}/{len(session_dirs)} : {session_dir}")
        df = cvdata.session_dataframe(session_dir)
        if df is None:
            print(f"  WARNING : {session_dir} is dodgy, skipping")
            dodgy_sessions.append(session_dir)
            continue

        cvdata.save_session_to_parquet(df, session_dir)

    cvdata.write_dodgy_sessions(dodgy_sessions, patient_id)
