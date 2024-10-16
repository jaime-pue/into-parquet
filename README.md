# into-parquet

CLI tool for giving .csv files a schema and cast them to .parquet.

By default, will read all files recursively from `./data/input/`

It will overwrite previous parquet files.

## Usage

Create a folder structure, like:

```
Root/
└── data/
    ├── input/
    └── output/
```
Compile with [maven](https://maven.apache.org/):

```shell
mvn clean package
```

Compiles to jar type:

```shell
java -jar into-parquet-0.0.2-jar-with-dependencies.jar --optional-flags
```

## Folder structure

### Input Folder

Put both files, csv file and text file with schema, inside input folder.

```
Root/
└── data/
    ├── input/
    │   ├── fileOne
    │   ├── fileOne.csv
    │   ├── fileTwo
    │   ├── fileTwo.csv
    │   └── fileThree.csv
    └── output
```

#### Csv files

Csv files with header

#### Table description

Text files with schema following the convention: `column_name type_of`

Example

```text
id int COMMENT 'represents field id'
name string COMMENT 'just a name'
flag BOOLEAN COMMENT 'boolean flag'
```

### Output Folder

Script will output parquet files, by default, to `./data/output/`

Name will be the same as original csv file

```
Root/
└── data/
    ├── input/
    │   ├── fileOne
    │   ├── fileOne.csv
    │   ├── fileTwo
    │   ├── fileTwo.csv
    │   └── fileThree.csv
    └── output/
        ├── fileOne/
        │   ├── ._SUCCESS.crc
        │   ├── .part-hash-snappy.parquet.crc
        │   ├── _SUCCESS
        │   └── part-hash-snappy.parquet
        ├── fileTwo/
        │   ├── ._SUCCESS.crc
        │   ├── .part-hash-snappy.parquet.crc
        │   ├── _SUCCESS
        │   └── part-hash-snappy.parquet
        └── fileThree/
            ├── ._SUCCESS.crc
            ├── .part-hash-snappy.parquet.crc
            ├── _SUCCESS
            └── part-hash-snappy.parquet
```


## CLI Options

| Name          | Shortcut | Comment                                        | Type   | Example                        |
|---------------|----------|------------------------------------------------|--------|--------------------------------|
| `--files`     | `-f`     | List of files for processing, separated by `;` | String | `--file fileOne;fileTwo;fileN` |
| `--mode`      | `-m`     | Cast method                                    | String | `--mode raw`                   |
| `--path`      | `-p`     | Path where csv files are                       | String | `--path ./path/to/input/`      |
| `--output`    | `-o`     | Where to put parquet files                     | String | `-output ~/output/dir/`        | 
| `--fail-fast` |          | Fail if any error found                        | Flag   | `--fail-fast`                  |
| `--version`   | `-v`     | Show current script version                    | Flag   | `--version`                    |
| `--help`      | `-h`     | Show help context                              | Flag   | `--help`                       |

### Cast method options

#### Raw Schema

Read all fields as String and don't perform any inference or cast to anything

#### Infer Schema

Infer schema automatically and try casting each column to appropriate type.

May infer types wrong, such as inferring a String field as an Integer. "00002" could be cast to 2

#### Parse Schema

Apply a given schema from a text file with the same name as the processed csv file

