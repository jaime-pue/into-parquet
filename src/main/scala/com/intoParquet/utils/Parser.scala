package com.intoParquet.utils

import scopt.OptionParser
object Parser {

    case class InputArgs(
        csvFile: Option[String]
    )

    private final val parser = new OptionParser[InputArgs]("into-parquet") {
        head("Cast csv files to parquet format", "0.0.1")
        opt[String]('f', "files").optional
            .action((inputDate, c) => c.copy(csvFile = Some(inputDate)))
            .text("csv files for processing, separated by ';'")
    }

    def parseSystemArgs(args: Array[String]): Option[InputArgs] = {
        parser.parse(args, InputArgs(csvFile = None))
    }
}
