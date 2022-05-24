"""Evaluation script for Eval.AI task

This script will calculate ROC AUC metrics for each patient, as well as an average metric across
all patients.

Usage:

    $ python3 scripts/evaluation.py

Assumes that contestant has written a script that can generate predictions from their model for
files in `./data/val` (which they don't have access to). Files in `./data/val` follow the same
structure as seen in `./data/train` and `./data/test`:

    `./data/val/PATIENT_ID>/<SESSION_ID>/UTC-YYYY_MM_DD-hh_mm_ss.parquet`,

where <PATIENT_ID> is one of "1110", "1869", "1876", "1904", "1965", and "2002".

Predictions should be generated and recorded in a csv file with the following format:
```
filepath, prediction
val/<PATIENT_ID>/<SESSION_ID>/UTC-YYYY_MM_DD-hh_mm_ss.parquet, <PREDICTION>
...
```
"""

import logging
from pathlib import Path

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from sklearn.metrics import roc_curve, roc_auc_score

PREDICTIONS_PATH = "./predictions.csv"
OUTPUT_DIR = "./data/output"

LABELS_DIR = "./data/artifacts"
PATIENT_IDS = ["1110", "1869", "1876", "1904", "1965", "2002"]

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# Configure logging
logger = logging.getLogger()
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(name)s: %(message)s',
    level=logging.INFO,
    force=True,
)

logger.info(f"Loading predictions from {PREDICTIONS_PATH}")
assert Path(PREDICTIONS_PATH).exists(), \
    f"File at {PREDICTIONS_PATH = } does not exist."
input_csv = pd.read_csv(PREDICTIONS_PATH)

logger.info(f"Verifying predictions input")
assert input_csv.columns.tolist() == ['filepath', 'prediction'], \
    f"Incorrect columns in {PREDICTIONS_PATH}, expected 'filepath, prediction'."
assert not input_csv.empty, \
    "Empty predictions file loaded."
assert (input_csv['prediction'] <= 1).all() and (input_csv['prediction'] >= 0).all(), \
    f"Invalid predictions detected in {PREDICTIONS_PATH}."
assert (input_csv['filepath'].apply(lambda fp: fp.split('/')[0]) == 'val').all(), \
    "Expected first dir of each entry in 'filepath' to begin with split 'val'."
assert (input_csv['filepath'].apply(lambda fp: fp.split('/')[1]).isin(PATIENT_IDS)).all(), \
    "Expected second dir in each entry in 'filepath' to have valid patient_id."

logger.info("Loading validation labels for all patients")
results = pd.DataFrame()
for pid in PATIENT_IDS:
    labels = pd.read_csv(Path(LABELS_DIR) / f'{pid}_val_labels.csv')
    results = pd.concat([results, labels])

logger.info("Checking all validation files have a prediction")
results = results.sort_values('filepath').reset_index(drop=True)
input_csv = input_csv.sort_values('filepath').reset_index(drop=True)
assert input_csv['filepath'].equals(results['filepath']), \
    f"Expected {PREDICTIONS_PATH} to contain predictions for all files in val splits."

results['prediction'] = input_csv['prediction']
results['patient_id'] = results['filepath'].apply(lambda fp: fp.split('/')[1])

metrics = [{'patient_id' : pid, 'ROC_AUC': None} for pid in PATIENT_IDS + ['all']]
metrics = pd.DataFrame(metrics)
metrics.set_index('patient_id', inplace=True)

for pid in metrics.index:
    logger.info(f"Calculating metrics for {pid}")
    selection = results[results['patient_id'] == pid] if pid != 'all' else results
    title = f"Patient {pid}" if pid != 'all' else "All patients"

    fpr, tpr, _ = roc_curve(selection['label'], selection['prediction'])
    auc = roc_auc_score(selection['label'], selection['prediction'])
    metrics['ROC_AUC'][pid] = auc

    trace = go.Scatter(
        x=tpr,
        y=fpr,
        mode='lines',
    )
    layout = {
        'title': " ".join([title, "validation ROC", f"(AUC = {auc})"]),
        'xaxis_title': 'TPR',
        'yaxis_title': 'FPR',
        'showlegend': False,
    }
    fig = go.Figure(
        data=[trace],
        layout=layout,
    )
    fig.write_image(Path(OUTPUT_DIR) / f'roc_{pid}.png')

logger.info(f"Saving outputs to {OUTPUT_DIR}")
metrics.to_csv(Path(OUTPUT_DIR) / 'metrics.csv', index=False)

logger.info(f"PASS!")
