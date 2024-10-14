# into-parquet

CLI tool for giving .csv files a schema and cast them to .parquet.

It will overwrite previous parquet files.

## Usage

Create a folder structure:

```
Root/
└── data/
    ├── input/
    │   ├── raw
    │   └── schema
    └── output
```

### Input Folder

#### Raw

Csv files with header

#### Schema

Text files with schema following the convention: `column_name type_of`

Example

```text
id int COMMENT 'represents field id'
name string COMMENT 'just a name'
flag BOOLEAN COMMENT 'boolean flag'
```

## CLI Options

Files:

Pass files with `-f`, `--files` separated by `;`

Recursive folder:

Pass flag with `-R`, `--recursive`
