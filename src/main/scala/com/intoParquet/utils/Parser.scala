package com.intoParquet.utils

import com.intoParquet.model.enumeration.{InferSchema, Raw, ParseSchema, CastMode}
import scopt.OptionParser

object Parser {

    private val PacketDescription: String =
        "Converts csv format files into parquet files, and can apply a schema when transforming them."

    private val Version: String =
        s"""into-parquet 0.0.2
          |
          |$PacketDescription
          |
          |Copyright (C) 2024 Free Software Foundation, Inc.
          |License GPLv3+: GNU GPL version 3 or later <https://gnu.org/licenses/gpl.html>.
          |This is free software: you are free to change and redistribute it.
          |There is NO WARRANTY, to the extent permitted by law.
          |
          |Written by Jaime Alvarez Fernandez.
          |""".stripMargin

    case class InputArgs(
        csvFile: Option[String],
        castMethod: CastMode = ParseSchema,
        recursive: Boolean = true,
        inputDir: Option[String] = None,
        outputDir: Option[String] = None,
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
        head(Version)
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
                else failure("Cast mode should be one of the following: raw, r; infer, i; parse, p")
            )
            .text("""Choose one of the following: [R]aw, [I]nfer, [P]arse
                  | > Raw: read csv fields as String
                  | > Infer: infer schema from fields (May yield wrong types)
                  | > Parse: apply schema if found in adjacent text file
                  |""".stripMargin)
        opt[String]('p', "path").optional
            .action((path, c) => c.copy(inputDir = if (isEmpty(path)) None else Some(path)))
            .text("Path to input folder")
        opt[String]('o', "output").optional
            .action((path, c) => c.copy(outputDir = if (isEmpty(path)) None else Some(path)))
            .text("Path to output folder")
        opt[Unit]("fail-fast").optional
            .action((_, c) => c.copy(failFast = true))
            .text("Fail and exit if any transformation fails")
        checkConfig(c =>
            if (c.recursive && c.csvFile.isDefined)
                failure("Recursive flag and files are mutually exclusive options")
            else success
        )
        note("""
              |Other options:
              |""".stripMargin)
        help('h', "help").text("prints this usage text")
        version('v', "version").text("prints program version")
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
