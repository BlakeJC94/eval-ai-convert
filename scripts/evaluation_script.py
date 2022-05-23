"""Evaluation script for Eval.AI task

This script assumes contestant has written a script that can generate predictions from their model
for files in `./data/val` (which they don't have access to).

Files in `./data/val` follow the same structure as see in `./data/train` and `./data/test`:

    `./data/PATIENT_ID>/val/<SESSION_ID>/UTC-YYYY_MM_DD-hh_mm_ss.parquet`,

where <PATIENT_ID> is one of "1110", "1869", "1876", "1904", "1965", and "2002".

Predictions should be generated and recorded in a csv file with the following format:
```
filepath, prediction
<PATIENT_ID>/val/<SESSION_ID>/UTC-YYYY_MM_DD-hh_mm_ss.parquet, <PREDICTION>
...
```

This script will calculate ROC AUC metrics for each patient, as well as an average metric across
all patients.
"""

from pathlib import Path
import logging

import pandas as pd
import numpy as np
from sklearn.metrics import roc_curve, roc_auc_score
from matplotlib import pyplot as plt

PREDICTIONS_PATH = "./predictions.csv"
OUTPUT_DIR = "./"
PATIENT_IDS = ["1110", "1869", "1876", "1904", "1965", "2002"]

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

logger = logging.getLogger()
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(name)s: %(message)s',
    level=logging.INFO,
    force=True,
)

input_csv = pd.read_csv(PREDICTIONS_PATH)

assert input_csv.columns == ['filepath', 'predictions'], \
    f"Incorrect columns in {PREDICTIONS_PATH}, expected 'filepath, predictions'."
assert not input_csv.empty, \
    "Empty predictions file loaded."
assert (input_csv['predictions'] <= 1).all() and (input_csv['predictions'] >= 0).all(), \
    f"Invalid predictions detected in {PREDICTIONS_PATH}."
assert (input_csv['filepath'].apply(lambda fp: fp.split('/')[0]).isin(PATIENT_IDS)).all(), \
    f"Expected start of each entry in 'filepath' to begin with valid patient_id."

# load all labels for validation split
results = pd.DataFrame()
for pid in PATIENT_IDS:
    labels = pd.read_csv(f'./data/artifacts/{pid}_val_labels.csv')
    results = pd.concat([results, labels])

# sort values in both dataframes
results = results.sort_values('filepath')
input_csv = input_csv.sort_values('filepath')

assert input_csv['filepath'].equals(results['filepath']), \
    f"Expected {PREDICTIONS_PATH} to contain predictions for all files in val splits."

# load predictions into dataframe
results['predictions'] = None
results['predictions'] = input_csv['predictions']

# add `patient_id` column to dataframe
results['patient_id'] = results['filepath'].apply(lambda fp: fp.split('/')[0])

# init metrics dataframe
metrics = [{pid: None} for pid in PATIENT_IDS + ['all']]
metrics = pd.DataFrame(metrics, columns=['patient_id', 'ROC_AUC'])
metrics.set_index('patient_id', inplace=True)

for pid in metrics.index:
    selection = results[results['patient_id'] == pid] if pid != 'all' else results

    # Calculate metrics
    fpr, tpr, _ = roc_curve(selection['labels'], selection['predictions'])
    auc = roc_auc_score(selection['labels'], selection['predictions'])
    metrics['ROC_AUC'][pid] = auc

    # Plot and save roc curve
    plt.plot(fpr, tpr, label=f"{auc = }")
    plt.title(f"ROC for validation data for patient {pid}")
    plt.ylabel('TPR')
    plt.xlabel('FPR')
    plt.legend(loc=4)
    plt.savefig(Path(OUTPUT_DIR) / f'roc_{pid}.png')

# save results
metrics.to_csv(Path(OUTPUT_DIR) / 'metrics.csv')
