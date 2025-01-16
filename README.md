# into-parquet

![GitHub License](https://img.shields.io/github/license/Jaime-alv/into-parquet?color=0)
![GitHub Tag](https://img.shields.io/github/v/tag/Jaime-alv/into-parquet?color=009EDB)

![Workflow](https://github.com/Jaime-alv/into-parquet/actions/workflows/maven.yml/badge.svg)

## About

This command-line tool is designed to convert CSV files into the Parquet format.
It automatically scans the specified input directory for all CSV files, processes them, and saves the converted
Parquet files in a target location.

By default, will read all files recursively from `./data/input/`

It will overwrite previous Parquet files.

This utility is particularly useful for users who need to work with local datasets.
The conversion process preserves column names, and the structure of the original CSV files while transforming them
into Parquet format with new data types defined by the user.

When the script encounters an exception, it will attempt to skip to the next CSV file and continue the
transformation process.
This behaviour ensures that the script can process multiple files in one go even if one file contains issues.
A log message will be rendered to the user, so it can take future actions with said file.

However, if the `fail-fast` mode is enabled, the script will immediately stop its execution upon encountering any error,
rather than proceeding with the next file. A full traceback will be displayed back to the user.

## Requirements

- Java 1.8 or 1.11
- Maven 3

## Usage

### Pre-requisites:

1. Clone or download this repository

```shell
git clone git@github.com:Jaime-alv/into-parquet.git
```

2. Create a folder structure, like:

```
Root/
└── data/
    ├── input/
    └── output/
```

3. Compile with [maven](https://maven.apache.org/):

```shell
mvn package clean -Dmaven.test.skip
```

> `package` will compile the CLI tool
>
> `clean` will delete /target directory
> 
> `-Dmaven.test.skip` skip tests

No need to re-compile unless a new version is needed or some changes applied to source code.

NOTE:` mvn install` will launch scoverage report

### Execution

Once compiled. Execute jar within the shell with a command like:

```shell
java -jar into-parquet-cli.jar
```

The tool supports a full range of [optional flags](#cli-options) the user can add, e.g.:

```shell
java -jar into-parquet-cli.jar --files exampleFile --fallback raw
```

Ensure that all your CSV files, with the [mandatory header row](#mandatory-header-row), are placed in the `/data/input`
folder along with the [table description file](#table-description), which should be in the same directory as this JAR
file.
The tool will automatically read all CSV files from this folder and convert them into Parquet format.

## Supported data types

This script only supports flat, non-nested data structures.
Please ensure that the data provided is not hierarchical or nested in any way, as it cannot be processed.
The data types should correspond to standard SQL types, such as `STRING`, `INT`, `DATE`, and so on.
Unsupported data types will trigger an exception, which will specify the file containing the unsupported type.

If the "fail-fast" mode is enabled, the application will immediately terminate upon encountering an unsupported type,
and throw the correspondent
exception [[NotImplementedTypeException](src/main/scala/com/github/jaime/intoParquet/exception/AppException.scala)].
On the contrary, if "fail-fast" is not enabled, the script will skip to the next CSV file and attempt to process it,
continuing the transformation process for the remaining files.

### SQL Supported data types

The next table shows the currently supported data types:

| SQL name        |
|-----------------|
| string          |
| boolean         |
| timestamp       |
| date            |
| byte, tinyint   |
| smallint, short |
| int, integer    |
| bigint, long    |
| double          |
| float, real     |
| decimal         |

#### Note on Decimal type

The decimal type requires both the precision and the scale magnitudes.
Scale magnitude can't be higher than precision magnitude.

Examples: `Decimal(38,2)`, `decimal(10, 4)`, `decimal (10,2)`

## Folder structure

### Input Folder

Put both files, csv file and text file with schema (the table description), inside input folder.

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

Csv files with the mandatory header row

### <span style="color:red">WARNING</span>

#### Mandatory header row

CSV files must include a mandatory header row to ensure proper functionality. This header, located at the top of the
file, defines the names of the columns, allowing for correct interpretation of the data by the software. Without a
header, it's impossible to guess which column maps to which type.

#### Note on `Null` values

By default, the into-parquet script handles null values in the input CSV files. Any missing or empty values in the CSV
will be interpreted as nulls during the conversion process.
To ensure proper handling, it is important that any null values in the CSV are represented as NULL in uppercase.
This ensures consistency and allows the script to correctly recognise and convert these null values into the Parquet
format.

#### Date format

The date input format must follow the ISO 8601 standard, which specifies the structure as **YYYY-MM-DD**, where "YYYY"
represents the four-digit year, "MM" the two-digit month (01 through 12), and "DD" the two-digit day of the month (01
through 31),
and the separator must be a hyphen "-".
For example, 2024-12-02 indicates the 2nd of December, 2024.

The same happens with the timestamp format. Therefore, the order of the elements used to express date and time in ISO
8601 is
as follows: year, month, day, hour, minutes, seconds, and milliseconds.

For example, September 27, 2022 at 6 p.m. is represented as 2022-09-27 18:00:00.000.

#### Table description

Plain text file following the convention for column names and their expected data types, `column_name type_of`. The file
does not require a specific file extension, but the name must be the same as the CSV file the user needs to transform.
To facilitate proper formatting, users can retrieve the expected schema, including column names and their corresponding
data types, by using the `DESCRIBE TABLE` command in Impala. This ensures that the file aligns precisely with the
anticipated structure, enabling seamless integration and data conversion. The first line, `name type comment`, is
optional. [Supported data types](#sql-supported-data-types).

Example

```text
name type comment
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

| Name          | Shortcut | Comment                                                        | Type   | Example                         |
|---------------|----------|----------------------------------------------------------------|--------|---------------------------------|
| `--files`     | `-f`     | List of files for processing, by default, separated by `,`     | String | `--files fileOne,fileTwo,fileN` |
| `--sep`       |          | Field separator, for special bash chars they should be escaped | String | `--sep \;`                      |
| `--path`      | `-p`     | Path where csv files are                                       | String | `--path ./path/to/input/`       |
| `--output`    | `-o`     | Where to put parquet files                                     | String | `-output ~/output/dir/`         |
| `--mode`      | `-m`     | Cast method                                                    | String | `--mode raw`                    |
| `--fallback`  | `-fb`    | Fallback method                                                | String | `--fallback fail`               |
| `--fail-fast` |          | Fail if any error found                                        | Flag   | `--fail-fast`                   |
| `--debug`     |          | Show debug level messages                                      | Flag   | `--debug`                       |
| `--version`   | `-v`     | Show current script version                                    | Flag   | `--version`                     |
| `--help`      | `-h`     | Show help context                                              | Flag   | `--help`                        |

### Example

```shell
java -jar into-parquet-cli.jar --files fileOne,fileTwo --output /home/user/
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

## Debug Mode

### Enabling Debug Mode in the Application

To enable debug mode for the application, you will need to pass `--debug` flag as an argument:

```shell
java -jar into-parquet-cli.jar --debug
```

### Customising Log Rendering

Additionally, you can customise how the logs are rendered to suit your needs.
The log4j2.xml file allows you to modify the layout and format of log messages.
For example, you can change the log format to include timestamps, log levels, or thread information, depending on what
you need to track.

Here is an example of how to customise the log format:

```xml

<Appenders>
    ...
    <Console name="app" target="System.out">
        <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss} [%t] [%-5p] %c{1} - %m%n"/>
    </Console>
</Appenders>
```

This will render the log output in a more readable format, including the date, thread name, log level, logger name, and
the actual log message.

Feel free to explore further customisation options based on your logging requirements.
You can refer to [the official Log4j2 documentation](https://logging.apache.org/log4j/2.x/manual/layouts.html) for more
detailed configuration
possibilities.
