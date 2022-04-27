# eval-ai-convert

Scripts for converting Eval AUI dataset

## Setup

Create a link to the dataset:

```bash
$ ln -s <path-to-eval-al-data> ./data/edf
$ ln -s <path-to-eval-al-sztimes> ./data/sztimes
```

Expected file structure in `path-to-eval-al-data`:
```
  edf/1110/
  ├── testing
  │   ├── 1588104564
  │   │   ├── Empatica-ACC.edf
  │   │   ├── Empatica-BVP.edf
  │   │   ├── Empatica-EDA.edf
  │   │   ├── Empatica-HR.edf
  │   │   └── Empatica-TEMP.edf
  │   ...
  └── training
      ├── 1582758851
      │   ├── Empatica-ACC.edf
      │   ├── Empatica-BVP.edf
      │   ├── Empatica-EDA.edf
      │   ├── Empatica-HR.edf
      │   └── Empatica-TEMP.edf
      ...
```

Expected file structure in `path-to-eval-al-sztimes`:
```
  sztimes/
  ├── 1110.pkl
  ├── 1869.pkl
  ...
  └── 2002.pkl
```

Create a link to another directory to store outputs:
```bash
$ ln -s <path-to-save-converted-data> ./data/parquet
$ ln -s <path-to-save-artifacts> ./data/artifacts
```

Install the python package:
```bash
$ pip install -e .
```

## Usage

Check docs in CLI:
```bash
# Get list of commands:
$ eadata --help
# Get help for particular command:
$ eadata convert --help
```


