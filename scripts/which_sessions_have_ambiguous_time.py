from pathlib import Path
import warnings
from datetime import datetime

from pytz import timezone
import mne
from tqdm import tqdm
import pandas as pd

from ea_convert import EDF_PATH, ARTIFACTS_PATH

mne.set_log_level(False)

AMBIGUOUS_TIMES = [
    '3 November 2019, 1:00:00 am',
    '1 November 2020, 1:00:00 am',
    '7 November 2021, 1:00:00 am',
    '6 November 2022, 1:00:00 am',
]


def get_start_date(fp):
    tz = None
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        try:
            f = mne.io.read_raw_edf(str(fp))
            return f.info['meas_date'].replace(tzinfo=tz)
        except:
            return None


edf_files = [i for i in Path(EDF_PATH).glob('**/*') if i.suffix == '.edf' and i.name[0] != '.']
starts = []
for i in tqdm(edf_files, ncols=80):
    starts.append(get_start_date(i))

df = pd.DataFrame.from_dict({'filepath': edf_files, 'start': starts})

ambiguous_times = [datetime.strptime(t, '%d %B %Y, %I:%M:%S %p') for t in AMBIGUOUS_TIMES]
ambiguous_times = pd.Series(ambiguous_times)

mask = df.start.dt.floor('H').isin(ambiguous_times.dt.floor('H'))
result = df.filepath[mask]
if mask.sum() == 0:
    print('None of the local start datetimes in the edfs are ambiguous')
else:
    print('Files with ambigious start times found:')

    output_path = Path(ARTIFACTS_PATH) / 'files_with_ambiguous_start_times.txt'
    output_path.parent.mkdir(exist_ok=True, parents=True)
    with open(str(output_path), 'w') as f:
        for i in result:
            print(i)
            f.write(str(i) + '\n')
