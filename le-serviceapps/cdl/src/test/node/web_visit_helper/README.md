Web Visit Helper Scripts
========

Helper scripts for answering questions such as **which records contribute to total web visit to google.com in last 2 weeks**.

## Prerequisites

1. **NodeJS**: tested with version `11.7.0`. recommend to install using [nvm](https://github.com/nvm-sh/nvm)

## Installation

1. **Install NodeJS libraries**:
    - `npm install`
2. **Setup alias for helper scirpts (Optional)**:
    - `sudo chmod +x index.js`
    - `npm link`
3. **Setup example data (Optional)**:
    - `bash setup_example.sh`
    - data will be decompressed to `./example_data`, including `raw stream`, `daily agg`, `metrics` and `dimension metadata`.
    - skip if you want to try with real data directly

## Usage

If alias has been setup in the last step, use `wvctl`, otherwise use `node index.js`. This section assumes the alias has been setup.

#### 1. Show usage

`wvctl -h`: to show all available sub commands

Example result:
```
LE-660420:web_visit_helper slin$ wvctl -h
Usage: wvctl [options] [command]

Options:
  -V, --version             output the version number
  -h, --help                output usage information

Commands:
  raw-to-daily [options]
  daily-to-json [options]
  daily-diff [options]
  daily-to-stats [options]
  raw-to-stats [options]
```

`wvctl <sub-cmd> -h`: to show all options of this sub command. e.g., `wvctl raw-to-daily -h`

Example result:
```
LE-660420:web_visit_helper slin$ wvctl raw-to-daily -h
Usage: wvctl raw-to-daily [options]

Options:
  -f, --format <format>     Input format, "csv" means single csv file and "avro" means avro directory
  -s, --src <src>           Source path, either a file/directory depends on format
  -d, --dst <dst>           Destination path, either a file/directory depends on format
  -D, --dim-path <dimPath>  Dimension metadata json file
  -h, --help                output usage information
```

#### 2. Calculate metrics from raw stream table (avro)

Calculate total visit per `source medium + path pattern`, `path pattern`, `source medium` for each account. Also can know which records contribute to the final visit number.

**Usage**:
```
Usage: wvctl raw-to-stats [options]

Calculate metrics from raw stream data (avro table)

Options:
  -s, --src <src>               Source path for raw stream avro directory
  -D, --dim-path <dimPath>      Dimension metadata json file
  -t, --tmp <tmp>               Tmp directory for all intermediate files
  -d, --dst <dst>               Destination stats/records directory
  -S, --start-date <startDate>  Start date string (inclusive), format yyyy-MM-dd
  -E, --end-date <endDate>      End date string (inclusive), format yyyy-MM-dd
  -h, --help                    output usage information
```

**Example**:

1. Using data setup from installation step. Trying to compute stats between `2019-09-02` to `2019-09-23`.

`wvctl raw-to-stats -s ./example_data/RawStream_as_ptvkcfols7szegrsv_daxq_2019-12-09_09-01-22_UTC/ -D ./example_data/dim_metadata.json -d hello -S 2019-09-02 -E 2019-09-23`

2. Calculated metrics will be in `./stats/<dir passed in as -d flag>`. There will be three json files (shown below).

```
LE-660420:web_visit_helper slin$ tree stats
stats
└── hello
    ├── ptn_stats.json
    ├── sm_ptn_stats.json
    └── sm_stats.json

1 directory, 3 files
```

`ptn_stats.json` contains the metrics per path pattern, structure is as follows:
```
{
    "stats": {
        "2": {
            "199531": 39,
            "199535": 4,
            "199633": 1,
            "199696": 2
        },
        "<PathPatternId>": {
            "AccountId": #Visits
        }
    }
}
```
`sm_stats.json` contains metrics per source medium with similar structure.
`sm_ptn_stats.json` contains metrics per sm/ptn combination.
```
{
    "stats": {
        "PtnId=7,SmId=5": {
            "199531": 281,
            "199533": 1,
            "199535": 15
        }
    }
}
```

3. Records contributed to each metric will be saved to `./records/<dir passed in as -d flag>` directory.

```
LE-660420:web_visit_helper slin$ tree records/
records/
└── hello
    ├── input_metadata.json
    ├── ptn_records_id_12.csv
    ├── ptn_records_id_14.csv
    ...
    ├── sm_ptn_records_PtnId=12,SmId=1.csv
    ├── sm_ptn_records_PtnId=12,SmId=18.csv
...
```

Records for `ptn_stats.json` will be in `ptn_records_id_<PathPatternId>.csv`, `sm_ptn_stats.json` will be in `sm_ptn_records_PtnId=<PathPatternId>,SmId=<SourceMediumId>.csv`, etc.

#### 3. Use Data from Actual Tenants

1. Recommend to use the command from previous section (`wvctl raw-to-stats`). Raw stream data can be found in HDFS or S3, download them to local.

2. Dimension metadata can be obatined by the API below.
```
GET https://<PRIVATE_URL>/cdl/customerspaces/<TENANT>/activities/dimensionMetadata/streams/WebVisit

Example Request:
https://<PRIVATE_URL>/cdl/customerspaces/CDL_QA_Lina_webactivity_8/activities/dimensionMetadata/streams/WebVisit

Response:
{
    "PathPatternId": {
        "dimension_values": [
            {
                "PathPatternId": "12",
                "PathPattern": "https://www.shop.basf.com.br/carecreations/pt/BRL/my-account/orders",
                "PathPatternName": "Orders"
            },
            ...
        ],
        "cardinality": 10
    },
    "SourceMediumId": {
        "dimension_values": [
            {
                "SourceMediumId": "8",
                "SourceMedium": "newsletter9-2014/Email"
            },
            ...
        ],
        "cardinality": 10
    }
}
```

3. Start date & end date depends on tenant's configuration so better to get it with java code or API prior to using this script.

4. Set destination directory with `-d destination_dir`.

5. Input daily directory and start/end date will be recorded in `records/<Destination Dir>/input_metadata.json`

#### 4. Other Commands for Debugging

use show usage and try to figure out for now

**TODO** add doc
