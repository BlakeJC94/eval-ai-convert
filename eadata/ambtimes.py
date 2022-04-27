"""Query for potential timezone issues.

All of the start times in the EDF files are "local time" accoring to Daniel, which would mean
they're in `US/Central` time presumably. There is a possibility that converting the timexzone to
UTC may fail if one of these start times fall within the "repeated" hour that occurs when the local
time is set back 1 hour in November.

This also looks through the sztimes annotations for ambiguous times as well.
"""

import logging
from typing import Optional
from pathlib import Path
from datetime import datetime

import pandas as pd

from .globals import PATIENT_IDS
from .paths import ARTIFACTS_PATH
from eadata.labels import load_sztimes
from eadata.analysis import get_file_start_times

logger = logging.getLogger(__name__)

AMBIGUOUS_TIMES = [
    '3 November 2019, 1:00:00 am',
    '1 November 2020, 1:00:00 am',
    '7 November 2021, 1:00:00 am',
    '6 November 2022, 1:00:00 am',
]

def ambtimes(patient_id: Optional[str] = None):
    """Check local times in edf files and sztimes for DST ambiguities.

    Ambigious times will be written to stdout and `.data/artifacts`

    Args:
        patient_id: If omitted, all EDF files will be checked.
    """
    if patient_id is None:
        logger.info("Analysing times for all patients")
        patient_ids = PATIENT_IDS.copy()
    else:
        assert patient_id in PATIENT_IDS, f"{patient_id} not in {PATIENT_IDS}"
        logger.info(f"Analysing times for patient {patient_id}")
        patient_ids = [patient_id]

    # Get sztimes
    sztimes_df = load_sztimes(patient_ids)

    # Get starts
    starts_df = get_file_start_times(patient_ids, multiproc=True)

    # Convert ambiguous times into a DataFrame as well
    ambiguous_times = [datetime.strptime(t, '%d %B %Y, %I:%M:%S %p') for t in AMBIGUOUS_TIMES]
    ambiguous_times = pd.Series(ambiguous_times)

    # find matches for ambiguous times in sztimes
    sztimes_mask = sztimes_df.local.dt.floor('H').isin(ambiguous_times.dt.floor('H'))
    if sztimes_mask.sum() == 0:
        logger.info('None of the seizures datetimes are ambiguous')
    else:
        logger.warning('Seizures with ambigious start times found:')
        output_path = Path(ARTIFACTS_PATH) / 'seizures_with_ambiguous_start_times.txt'
        output_path.parent.mkdir(exist_ok=True, parents=True)
        with open(str(output_path), 'w') as f:
            for i in sztimes_df[sztimes_mask].iterrows():
                out = f"{i.pid}, {i.local} ({i.utc})"
                logger.warning(out)
                f.write(out + '\n')

    # find matches for ambiguous times in start times
    starts_mask = starts_df.start.dt.floor('H').isin(ambiguous_times.dt.floor('H'))
    if starts_mask.sum() == 0:
        logger.info('None of the local start datetimes in the edfs are ambiguous')
    else:
        logger.warning('Files with ambigious start times found:')
        output_path = Path(ARTIFACTS_PATH) / 'files_with_ambiguous_start_times.txt'
        output_path.parent.mkdir(exist_ok=True, parents=True)
        with open(str(output_path), 'w') as f:
            for i in starts_df[starts_mask].iterrows():
                out = f"{i.pid}, {i.local} ({i.utc})"
                logger.warning(out)
                f.write(out + '\n')

