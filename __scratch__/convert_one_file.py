from itertools import combinations
import numpy as np
import pandas as pd

import pyarrow as pa
import pyarrow.parquet as pq

import mne


FILEDIR = './edf/1110/testing/1588104564/'
DTYPES = ['ACC', 'BVP', 'EDA', 'HR', 'TEMP']

mne.set_log_level(False)


# Obtain all files
all_files = {}
for dtype in DTYPES:
    all_files[dtype] = mne.io.read_raw_edf(f"{FILEDIR}/Empatica-{dtype}.edf")

# Get all time arrays
times = list(all_files[i].times for i in DTYPES)

# Get all channel names
ch_names = sum((all_files[i].ch_names for i in DTYPES), [])

# Concat all data arrays
# data = tuple(all_files[i].get_data() for i in DTYPES)
# data = np.concatenate(tuple(data), axis=0)  # ERROR: not all files are the same length!!

# Get pairs of channels that have unequal time vectors
chs_unequal_times = [
    (i, j) for (i, t1), (j, t2) in combinations(zip(DTYPES, times), 2)
    if not np.array_equal(t1, t2)
]
times_equal = (len(chs_unequal_times) == 0)
if not times_equal:
    print('Not all files have the same length/timesteps')

# Get pairs of channels that have unequal start times
starts = [all_files[i].info['meas_date'] for i in DTYPES]
chs_unequal_starts = [
    (i, j) for (i, t1), (j, t2) in combinations(zip(DTYPES, starts), 2)
    if not t1 == t2
]
starts_equal = (len(chs_unequal_starts) == 0)
if not starts_equal:
    print('Not all files have the same length/timesteps')

# Concat all data arrays after trimming to same length
data = tuple(all_files[i].get_data().transpose() for i in DTYPES)
min_data_len = min(d.shape[0] for d in data)
data = tuple(d[:min_data_len, :] for d in data)  # trim the data
data = np.concatenate(tuple(data), axis=1)

# Concat time vector and create dataframe
df_time = times[0][:min_data_len].reshape(-1, 1)
df_data = np.concatenate((df_time, data), axis=1)
df = pd.DataFrame(df_data, columns=['Time'] + ch_names)

# Convert to pyarrow table and save
table = pa.Table.from_pandas(df)
pq.write_table(table, 'example.pq')

# Load pq table and compare to df input
out = pd.read_parquet('example.pq')
assert out.equals(df)

