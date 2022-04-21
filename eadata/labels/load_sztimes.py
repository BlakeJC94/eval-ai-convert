import logging
from pickle import load
from typing import Optional, Union, List

import pandas as pd

from eadata.globals import PATIENT_IDS
from eadata.paths import SZTIMES_PATH

logger = logging.getLogger(__name__)


def load_sztimes(patient_ids: List[str]) -> pd.DataFrame:
    """Load seizure times from pickled dataframe."""

    df = []
    for pid in patient_ids:
        with open(SZTIMES_PATH / f'{pid}.pkl', 'rb') as f:
            sztimes = load(f)
            sztimes['local'] = sztimes['local'].dt.tz_localize(None)
            sztimes['pid'] = pid
            df.append(sztimes)

    df = pd.concat(df, axis=0)
    return df
