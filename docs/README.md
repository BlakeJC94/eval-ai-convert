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

Check times in files and labels for DST ambiguities:
```bash
$ eadata ambtimes [pid]
```
* If `pid` is omitted, all data will be analysed.
* Ambiguious times will be written to stdout and `./data/artifacts`

Convert data from edf to parquet:
```bash
$ eadata convert <pid>
```
* output will be saved as 1-min parquet files in `./data/parquet/<pid>/<session_timestamp>`
* Problematic sessions will be recorded to `./data/artifacts` if detected
* Set `--multiproc False` to disable parallel processing

Generate labels csv mapping parquet files to integers:
```bash
$ eadata labels
```

