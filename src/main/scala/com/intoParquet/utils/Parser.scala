package com.intoParquet.utils

import com.intoParquet.model.enumeration.{InferSchema, Raw, ReadSchema, WriteMode}
import scopt.OptionParser

object Parser {

    case class InputArgs(
        csvFile: Option[String],
        writeMethod: WriteMode = ReadSchema,
        recursive: Boolean = true
    )

    private final val parser = new OptionParser[InputArgs]("into-parquet") {
        head("Cast csv files to parquet format", "v0.0.1")
        opt[String]('f', "files").optional
            .action((inputDate, c) => c.copy(csvFile = Some(inputDate), recursive = false))
            .text("csv files for processing, separated by ';'")
        opt[String]('m', "mode").optional
            .action((writeMethod, c) => c.copy(writeMethod = parseWriteMethod(writeMethod)))
            .validate(m =>
                if (isValidMethod(m)) success
                else failure("Write mode should be one of the following: [R]aw, [I]nfer, [P]arse")
            )
            .text("Choose one of the following: [R]aw, [I]nfer, [P]arse")
        checkConfig(c =>
            if (c.recursive && c.csvFile.isDefined)
                failure("Recursive flag and files are mutually exclusive options")
            else success
        )
        note("""
              |Default options:
              |>>> Recursive method is true.
              |>>> Write method set to ReadSchema.
              |""".stripMargin)
    }

    def parseSystemArgs(args: Array[String]): Option[InputArgs] = {
        parser.parse(args, InputArgs(csvFile = None))
    }

    private def isValidMethod(method: String): Boolean = {
        val validMethods = List("raw", "infer", "parse", "r", "i", "p")
        validMethods.contains(method.toLowerCase())
    }

    private def parseWriteMethod(value: String): WriteMode = {
        value.toLowerCase() match {
            case "raw"   => Raw
            case "r"     => Raw
            case "infer" => InferSchema
            case "i"     => InferSchema
            case "parse" => ReadSchema
            case "p"     => ReadSchema
        }
    }
}
