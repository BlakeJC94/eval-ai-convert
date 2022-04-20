from pathlib import Path
from datetime import datetime
from pickle import load

from pytz import timezone
from tqdm import tqdm
import pandas as pd

from ea_convert import PATIENT_IDS, SZTIMES_PATH, ARTIFACTS_PATH

AMBIGUOUS_TIMES = [
    '3 November 2019, 1:00:00 am',
    '1 November 2020, 1:00:00 am',
    '7 November 2021, 1:00:00 am',
    '6 November 2022, 1:00:00 am',
]

df = []
for pid in PATIENT_IDS:
    with open(SZTIMES_PATH + f'/{pid}.pkl', 'rb') as f:
        sztimes = load(f)
        sztimes['local'] = sztimes['local'].dt.tz_localize(None)
        sztimes['pid'] = pid
        df.append(sztimes)

df = pd.concat(df, axis=0)

ambiguous_times = [datetime.strptime(t, '%d %B %Y, %I:%M:%S %p') for t in AMBIGUOUS_TIMES]
ambiguous_times = pd.Series(ambiguous_times)

mask = df.local.dt.floor('H').isin(ambiguous_times.dt.floor('H'))
if mask.sum() == 0:
    print('None of the seizures datetimes are ambiguous')
else:
    print('Seizures with ambigious start times found:')
    output_path = Path(ARTIFACTS_PATH) / 'seizures_with_ambiguous_start_times.txt'
    output_path.parent.mkdir(exist_ok=True, parents=True)
    with open(str(output_path), 'w') as f:
        for i in df[mask].iterrows():
            out = f"{i.pid}, {i.local} ({i.utc})"
            print(out)
            f.write(out + '\n')
