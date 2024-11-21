# into-parquet

This command-line tool is designed to convert CSV files into the Parquet format.
It automatically scans the specified input directory for all CSV files, processes them, and saves the converted
Parquet files in a target location.

By default, will read all files recursively from `./data/input/`

It will overwrite previous Parquet files.

This utility is particularly useful for users who need to work with local datasets.
The conversion process preserves column names, and the structure of the original CSV files while transforming them
into Parquet format with new data types defined by the user.

By default, when the script encounters an exception, it will attempt to skip to the next CSV file and continue the
transformation process.
This behaviour ensures that the script can process multiple files in one go even if one file contains issues.
A log message will be rendered to the user, so it can take future actions with said file.

However, if the `fail-fast` mode is enabled, the script will immediately stop its execution upon encountering any error,
rather than proceeding with the next file. A full traceback will be displayed back to the user.

## Requirements

- Java 1.11
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
java -jar target/into-parquet-cli.jar --optional-flags
```

Ensure that all your CSV files are placed in the `/data/input` folder, which should be in the same directory as this JAR
file.
The tool will automatically read all CSV files from this folder and convert them into Parquet format.

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

Examples: `Decimal(38,2)`, `decimal(10, 4)`

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

Csv files with the mandatory header row -> [read more](#span-stylecolorredwarningspan)


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
java -jar target/into-parquet-cli.jar --files fileOne,fileTwo --output /home/user/
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

To enable debug mode for the application, you will need to modify a specific parameter in the `log4j2.xml` configuration
file. This file is located in the `src/main/resources` directory of your project.

1. Locate the `log4j2.xml` file:

Navigate to the src/main/resources folder in your project directory and open the `log4j2.xml` file.

2. Modify the Log Level:

In the `log4j2.xml` file, you will find a <Logger> element that controls the logging level.
To enable debug logging, change the level attribute to debug. It should look something like this:

```xml

<Loggers>
    ...
    <Logger name="com.github.jaime.intoParquet" level="debug" additivity="false">
        <AppenderRef ref="app"/>
    </Logger>
</Loggers>
```

This will configure the application to log detailed debug information, which can help with troubleshooting and
monitoring.

3. Save and Restart:

After saving the changes to the `log4j2.xml` file, recompile the application and start once again.

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
