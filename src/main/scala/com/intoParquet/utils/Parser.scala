package com.intoParquet.utils

import com.intoParquet.model.enumeration.{InferSchema, Raw, ParseSchema, CastMode}
import scopt.OptionParser

object Parser {

    case class InputArgs(
        csvFile: Option[String],
        castMethod: CastMode = ParseSchema,
        recursive: Boolean = true,
        directory: String = "./data",
        failFast: Boolean = false
    ) {
        override def toString: String = {
            s"""Configuration:
               |>>> Recursive mode: ${recursive.toString}
               |>>> Cast method: ${castMethod.toString}
               |""".stripMargin
        }
    }

    private final val parser = new OptionParser[InputArgs]("into-parquet") {
        head(
          "Cast csv files to parquet format",
          "version 0.0.2",
          "\ninto-parquet  Copyright (C) 2024  Jaime Álvarez Fernández"
        )
        opt[String]('f', "files").optional
            .action((inputFiles, c) => {
                if (isEmpty(inputFiles)) {
                    c.copy(csvFile = None)
                } else {
                    c.copy(csvFile = Some(inputFiles), recursive = false)
                }
            })
            .text("csv files for processing, separated by ';'")
        opt[String]('m', "mode").optional
            .action((castMethod, c) => c.copy(castMethod = parseCastMethod(castMethod)))
            .validate(m =>
                if (isValidMethod(m)) success
                else failure("Cast mode should be one of the following: [R]aw, [I]nfer, [P]arse")
            )
            .text("Choose one of the following: [R]aw, [I]nfer, [P]arse")
        opt[String]('p', "path").optional
            .action((path, c) => c.copy(directory = path))
            .text("Path to folder")
        opt[Unit]("fail-fast").optional
            .action((_, c) => c.copy(failFast = true))
            .text("Fail and exit if any process fails")
        checkConfig(c =>
            if (c.recursive && c.csvFile.isDefined)
                failure("Recursive flag and files are mutually exclusive options")
            else success
        )
        help("help").text("prints this usage text")
        note("""
              |Default options:
              |>>> Recursive method is true.
              |>>> Cast method set to ParseSchema.
              |""".stripMargin)
    }

    def parseSystemArgs(args: Array[String]): Option[InputArgs] = {
        parser.parse(args, InputArgs(csvFile = None))
    }

    private def isValidMethod(method: String): Boolean = {
        val validMethods = List("raw", "infer", "parse", "r", "i", "p")
        validMethods.contains(method.toLowerCase())
    }

    private def parseCastMethod(value: String): CastMode = {
        value.toLowerCase() match {
            case "raw"   => Raw
            case "r"     => Raw
            case "infer" => InferSchema
            case "i"     => InferSchema
            case "parse" => ParseSchema
            case "p"     => ParseSchema
        }
    }

    def isEmpty(value: String): Boolean = {
        value.trim.isEmpty
    }
}
