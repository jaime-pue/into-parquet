package com.intoParquet.utils

import com.intoParquet.model.{InferSchema, Raw, ReadSchema, WriteMode}
import scopt.OptionParser
object Parser {

    case class InputArgs(
        csvFile: Option[String],
        writeMethod: WriteMode = Raw,
        recursive: Boolean = false
    )

    private final val parser = new OptionParser[InputArgs]("into-parquet") {
        head("Cast csv files to parquet format", "0.0.1")
        opt[String]('f', "files").optional
            .action((inputDate, c) => c.copy(csvFile = Some(inputDate)))
            .text("csv files for processing, separated by ';'")
        opt[Unit]('R', "recursive").optional().action((_, c) => c.copy(recursive = true))
        opt[Unit]('r', "raw").optional().action((_, c) => c.copy(writeMethod = Raw))
        opt[Unit]('i', "infer").optional().action((_, c) => c.copy(writeMethod = InferSchema))
        opt[Unit]('p', "parse").optional().action((_, c) => c.copy(writeMethod = ReadSchema))
    }

    def parseSystemArgs(args: Array[String]): Option[InputArgs] = {
        parser.parse(args, InputArgs(csvFile = None))
    }
}
