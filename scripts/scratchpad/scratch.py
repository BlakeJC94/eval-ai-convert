
import warnings
from pytz import utc, timezone
from datetime import datetime, timedelta
from pathlib import Path
from tqdm import tqdm
import mne
import pyarrow as pa
import numpy as np
import pandas as pd


DATA_DIR = './edf'
PATIENT_ID = '2002'
SPLIT = 'training'

OUTPUT_DIR = './parquet'

SECONDS_PER_BLOCK = 60
BLOCK_GAP_SECONDS_THRESHOLD = 60

# Get sequence of directories
data_path = Path(DATA_DIR).expanduser() / PATIENT_ID / SPLIT
assert data_path.exists(), f"{data_path} does not exist!"
directories = sorted(data_path.iterdir())

_log_level = mne.set_log_level(False)

all_time_data = []
for directory in tqdm(directories):
    edf_files = [i for i in directory.glob('*') if i.suffix == '.edf' and i.stem[0] != '.']
    assert len(edf_files) > 0

    start_time, end_time = None, None
    for dtype in ['ACC', 'EDA', 'TEMP', 'BVP', 'HR']:
        fp = directory / f'Empatica-{dtype}.edf'
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            try:
                file = mne.io.read_raw_edf(fp)
            except:
                continue

        file_start = file.info.get('meas_date').replace(tzinfo=timezone('US/Central'))
        file_start = file_start.astimezone(utc)
        start_time = file_start if start_time is None else min(start_time, file_start)

        # file_end = file_start + timedelta(seconds=file.times[-1])
        file_end = file_start + timedelta(seconds=file.n_times * file.info['sfreq'])
        end_time = file_end if end_time is None else max(end_time, file_end)

    assert start_time is not None and end_time is not None
    all_time_data.append({'directory': directory, 'start_time': start_time, 'end_time': end_time})

times = pd.DataFrame(all_time_data).sort_values('directory')
assert (times.start_time > times.end_time.shift()).all()




# Start blocks
file_start = times.start_time[0]
block_start_time = file_start.replace(second=0)
# Generate paths and names for first block
date_dir = Path(OUTPUT_DIR) / PATIENT_ID / SPLIT / block_start_time.strftime('Data_%Y_%m_%d')
hour_dir = date_dir / block_start_time.strftime('%H')
block_name = block_start_time.strftime('UTC_%H_%M_00')
block_path = hour_dir / block_name

# n_channels = 3  # TODO update
# block_data = None
# block_data = np.zeros((SECONDS_PER_BLOCK * 128, n_channels))
# Prepend nans for first block if needed  MAYBE APPEND THIS TO THE FILE DATA INSTEAD
# if file_start.seconds > 0:
#     block_data[:file_start.seconds * 128, :].fill(np.nan)


# Iterate through files
for i in range(len(times)):
    file_start = times.start_time[i]
    file_end = times.end_time[i]

    # load file and data
    # TODO loop over all edf file dtypes
    # TODO handle dropouts across channels
    dtype = 'ACC'
    fp = times.directory[i] / f'Empatica-{dtype}.edf'
    file = mne.io.read_raw_edf(fp)
    data = file.get_data().transpose()

    if i == 0 and file_start.seconds > 0:
        # In the first file, prepend data with nans if needed
        padding = np.empty((file_start.seconds * 128, n_channels)).fill(np.nan)
        data = np.concatenate((padding, data), axis=1)

    # calculate number of blocks needed for this file
    # Iterate over each block, create the files and such
    # Handle last (fractional) block if needed
    # - check if next file_start is within the minute. If so, add to this block and continue






# start_time = times.start_time[0]
# block_data = np.empty((3, SECONDS_PER_BLOCK * 128))  # TODO update n_channels
# for i in range(len(times)):
#     n_blocks_in_file = SECONDS_PER_BLOCK
#     for j in range(n_block)

#     end_time = times.end_time[i]


#     # CASE: file i ends after the block ends
#     # -> create block
#     # ->
#     # CASE: file i ends before the next block starts
#     # -> Append data to block
#     # -> Continue to next file
#     if end_time - start_time < timedelta(seconds=SECONDS_PER_BLOCK):
#         # TODO loop over all edf file dtypes
#         # TODO handle dropouts across channels
#         dtype = 'ACC'
#         fp = times.directory[i] / f'Empatica-{dtype}.edf'
#         file = mne.io.read_raw_edf(fp)
#         data = file.get_data()


#         # continue to next file
#         # TODO what if the start of the next file is well beyond end of this file?
#         # - fill the gap between end of this file and end of block with zeros
#         # - start a new block
#         continue




# block_start_time = times.start_time[file_index]
# if times.end_time[file_index] - times.start_time[file_index] <

