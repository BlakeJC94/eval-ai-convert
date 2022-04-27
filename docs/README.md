# eval-ai-convert

Scripts for converting Eval AUI dataset

## Setup

Create a softlink to the dataset:

```bash
$ ln -s <path-to-eval-al-data> ./data/edf
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

Create a link to another directory to storethe parquet output:
```bash
$ ln -s <path-to-save-converted-data> ./data/parquet
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


