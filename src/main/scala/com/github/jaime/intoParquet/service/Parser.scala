/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.service

import com.github.jaime.intoParquet.configuration.text.AppInfo
import com.github.jaime.intoParquet.model.enumeration.CastMode
import com.github.jaime.intoParquet.model.enumeration.FallBack
import com.github.jaime.intoParquet.model.enumeration.FallBackFail
import com.github.jaime.intoParquet.model.enumeration.FallBackInfer
import com.github.jaime.intoParquet.model.enumeration.FallBackNone
import com.github.jaime.intoParquet.model.enumeration.FallBackRaw
import com.github.jaime.intoParquet.model.enumeration.InferSchema
import com.github.jaime.intoParquet.model.enumeration.ParseSchema
import com.github.jaime.intoParquet.model.enumeration.RawSchema
import com.github.jaime.intoParquet.service.Common.sanitizeString
import scopt.OptionParser

object Parser {

    private val Version: String = AppInfo.license

    case class InputArgs(
        csvFile: Option[String],
        castMethod: Option[CastMode] = None,
        fallBack: Option[FallBack] = None,
        recursive: Boolean = true,
        inputDir: Option[String] = None,
        outputDir: Option[String] = None,
        failFast: Boolean = false,
        debugMode: Boolean = false,
        separator: Option[String] = None
    )

    private final val parser = new OptionParser[InputArgs]("into-parquet") {
        head(Version)
        note("""Input/Output operations:""".stripMargin)
        opt[String]('f', "files").optional
            .action((inputFiles, c) => {
                if (isEmpty(inputFiles)) {
                    c.copy(csvFile = None)
                } else {
                    c.copy(csvFile = Some(inputFiles), recursive = false)
                }
            })
            .text("csv files for processing. By default, separated by ','")
        opt[String]("sep").optional
            .action((sep, c) => c.copy(separator = Some(sep)))
            .text("Field separator character")
        opt[String]('p', "path").optional
            .action((path, c) => c.copy(inputDir = if (isEmpty(path)) None else Some(path)))
            .text("Path to input folder")
        opt[String]('o', "output").optional
            .action((path, c) => c.copy(outputDir = if (isEmpty(path)) None else Some(path)))
            .text("Path to output folder")
        note("""
              |Transformation options:""".stripMargin)
        opt[String]('m', "mode").optional
            .action((castMethod, c) => c.copy(castMethod = Some(parseCastMethod(castMethod))))
            .validate(m =>
                if (isValidMethod(m)) success
                else failure("Cast mode should be one of the following: raw, r; infer, i; parse, p")
            )
            .text("""Choose one of the following: [R]aw, [I]nfer, [P]arse
                  | > Raw: read csv fields as String
                  | > Infer: infer schema from fields (may yield wrong types)
                  | > Parse: apply schema if found in adjacent text file
                  |""".stripMargin)
        opt[Unit]("fail-fast").optional
            .action((_, c) => c.copy(failFast = true))
            .text("Fail and exit if any transformation fails")
        opt[Unit]("debug").optional
            .action((_, c) => c.copy(debugMode = true))
            .text("Activate `debug` mode and display more information")
        opt[String]("fallback")
            .abbr("fb")
            .optional
            .action((fallback, c) => c.copy(fallBack = Some(parseFallBackMethod(fallback))))
            .text("""When using Parse mode option, use fallback method if no schema file found:
                  | > Raw: read csv fields as String
                  | > Infer: infer schema from fields (may yield wrong types)
                  | > None: skip conversion
                  | > Fail: fail if no text file found""".stripMargin)
        note("""
              |Other options:""".stripMargin)
        help('h', "help").text("prints this usage text")
        version('v', "version").text("prints program version")
        note(AppInfo.Example)
    }

    def parseSystemArgs(args: Array[String]): Option[InputArgs] = {
        parser.parse(args, InputArgs(csvFile = None))
    }

    private def isValidMethod(method: String): Boolean = {
        val validMethods = List("raw", "infer", "parse", "r", "i", "p")
        validMethods.contains(sanitizeString(method))
    }

    private def parseCastMethod(value: String): CastMode = {
        sanitizeString(value) match {
            case "raw"   => RawSchema
            case "r"     => RawSchema
            case "infer" => InferSchema
            case "i"     => InferSchema
            case "parse" => new ParseSchema()
            case "p"     => new ParseSchema()
        }
    }

    private def parseFallBackMethod(value: String): FallBack = {
        sanitizeString(value) match {
            case "infer" => FallBackInfer
            case "fail"  => FallBackFail
            case "pass"  => FallBackNone
            case _       => FallBackRaw
        }
    }

    def isEmpty(value: String): Boolean = {
        value.trim.isEmpty
    }
}
