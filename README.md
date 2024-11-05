# into-parquet

This command-line tool is designed to convert CSV files into the efficient Parquet format.
It automatically scans the specified input directory for all CSV files, processes them, and saves the converted
Parquet files in a target location.

By default, will read all files recursively from `./data/input/`

It will overwrite previous Parquet files.

This utility is particularly useful for users who need to efficiently store and query large datasets.
The conversion process preserves column names, and the structure of the original CSV files while transforming them
into Parquet format with new data types defined by the user.

## Requirements

- Java 1.8
- Maven 3

## Usage

Create a folder structure, like:

```
Root/
└── data/
    ├── input/
    └── output/
```

To run the script, execute the following commands:

1. Compile with [maven](https://maven.apache.org/):

```shell
mvn clean package
```

2. Once compiled. Execute jar with the following command:

```shell
java -jar target/into-parquet-0.0.5-jar-with-dependencies.jar --optional-flags
```

Ensure that all your CSV files are placed in the `/data/input` folder, which should be in the same directory as this JAR
file. The tool will automatically read all CSV files from this folder and convert them into Parquet format.

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

##### Note on `Null` values

By default, the into-parquet script handles null values in the input CSV files. Any missing or empty values in the CSV
will be interpreted as nulls during the conversion process. To ensure proper handling, it is important that any null
values in the CSV are represented as NULL in uppercase. This ensures consistency and allows the script to correctly
recognise and convert these null values into the Parquet format.

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

| Name          | Shortcut | Comment                                        | Type   | Example                         |
|---------------|----------|------------------------------------------------|--------|---------------------------------|
| `--files`     | `-f`     | List of files for processing, separated by `,` | String | `--files fileOne,fileTwo,fileN` |
| `--path`      | `-p`     | Path where csv files are                       | String | `--path ./path/to/input/`       |
| `--output`    | `-o`     | Where to put parquet files                     | String | `-output ~/output/dir/`         |
| `--mode`      | `-m`     | Cast method                                    | String | `--mode raw`                    |
| `--fallback`  | `-fb`    | Fallback method                                | String | `--fallback fail`               |
| `--fail-fast` |          | Fail if any error found                        | Flag   | `--fail-fast`                   |
| `--version`   | `-v`     | Show current script version                    | Flag   | `--version`                     |
| `--help`      | `-h`     | Show help context                              | Flag   | `--help`                        |

### Example

```shell
java -jar target/target/into-parquet-0.0.3-jar-with-dependencies.jar --files fileOne,fileTwo --output /home/user/
```

### Cast method options

Try to apply these transformations

#### Raw Schema

Read all fields as String and don't perform any inference or cast to anything

`--mode raw`

#### Infer Schema

Infer schema automatically and try casting each column to appropriate type.

May infer types wrong, such as inferring a String field as an Integer. "00002" could be cast to 2

`--mode infer`

#### Parse Schema (Default)

Apply a given schema from a text file with the same name as the processed csv file

`--mode parse`

### Fallback method options

If there are no correspondent txt files inside `input` folder, try to apply some other operation

#### Raw

Read all fields as String and don't perform any inference or cast to anything

`--fallback raw`

#### Infer Schema

Infer schema automatically and try casting each column to appropriate type.

May infer types wrong, such as inferring a String field as an Integer. "00002" could be cast to 2

`--fallback infer`

#### Pass (Default)

Do nothing with the current file and skip to the next one

`--fallback pass`

#### Fail

Fail with an exception if no schema file found, if fail-fast mode set, will force an exit

`--fallback fail`

