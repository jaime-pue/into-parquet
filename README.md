# into-parquet

CLI tool for giving .csv files a schema and cast them to .parquet.

By default, will read all files recursively from /data

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

Compiles to jar type:

```shell
java -jar into-parquet-0.0.1-jar-with-dependencies.jar --optional-flags
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

| Name          | Shortcut | Comment                                        | Type   |
|---------------|----------|------------------------------------------------|--------|
| `--files`     | `-f`     | List of files for processing, separated by `;` | String |
| `--mode`      | `-m`     | Cast method                                    | String |
| `--path`      | `-p`     | Path where `/data` folder is                   | String |
| `--fail-fast` |          | Fail if any error found                        | Flag   |

### Cast method options

#### Raw Schema

Read all fields as String and don't perform any inference or cast to anything

#### Infer Schema

Infer schema automatically and try casting each column to appropriate type.

May infer types wrong, such as inferring a String field as an Integer. "00002" could be cast to 2

#### Parse Schema

Apply a given schema from a text file with the same name as the processed csv file

