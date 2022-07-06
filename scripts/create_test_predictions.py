from pathlib import Path

import pandas as pd
import numpy as np


TEST_SPLIT_DIR = "./data/parquet/test"
PREDICTIONS_PATH = "./predictions.csv"


# test_split_files = [str(fp) for fp in Path(TEST_SPLIT_DIR).glob("**/*.parquet")]
test_split_files = ['/'.join(list(fp.parts[3:])) for fp in Path(TEST_SPLIT_DIR).glob("**/*.parquet")]

records = []
for file in test_split_files:
    record = {"filepath": file, "prediction": np.random.rand()}
    records.append(record)

predictions = pd.DataFrame(records)
predictions.to_csv(PREDICTIONS_PATH, index=False)
