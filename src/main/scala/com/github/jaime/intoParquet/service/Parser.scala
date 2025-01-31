/*
 * IntoParquet Copyright (c) 2024-2025 Jaime Alvarez
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
        csvFile: Option[String] = None,
        excludeCSV: Option[String] = None,
        castMethod: Option[CastMode] = None,
        fallBack: Option[FallBack] = None,
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
                    c.copy(csvFile = Some(inputFiles))
                }
            })
            .text("Process only these CSV files. Separated by ','")
        opt[String]('e', "exclude").optional
            .action((excludeFiles, c) => {
                if (isEmpty(excludeFiles)) {
                    c.copy(excludeCSV = None)
                } else {
                    c.copy(excludeCSV = Some(excludeFiles))
                }
            })
            .text("Exclude these CSV files. Separated by ','")
        opt[String]("sep").optional
            .action((sep, c) => c.copy(separator = Some(sep)))
            .text("CSV field separator character")
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
            .text("""Set the transformation method for CSVs. 
                  | Choose one from [Raw | Infer | Parse]
                  | > Raw: read all CSV fields as String
                  | > Infer: infer schema from fields (may yield wrong types)
                  | > Parse [Default]: apply schema if found in adjacent text file
                  |""".stripMargin)
        opt[Unit]("fail-fast").optional
            .action((_, c) => c.copy(failFast = true))
            .text("Fail and exit if any transformation fails. Show stacktrace error")
        opt[Unit]("debug").optional
            .action((_, c) => c.copy(debugMode = true))
            .text("Activate `debug` mode and display more information")
        opt[String]("fallback")
            .abbr("fb")
            .optional
            .action((fallback, c) => c.copy(fallBack = Some(parseFallBackMethod(fallback))))
            .text("""When using Parse mode option, use fallback method transformation if no schema file found:
                  | Choose one from [raw | infer | none | fail]
                  | > Raw: read all CSV fields as String
                  | > Infer: infer schema from fields (may yield wrong types)
                  | > None [Default]: skip conversion
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

    private def parseCastMethod(value: String): CastMode = {
        sanitizeString(value) match {
            case "raw"   => RawSchema
            case "infer" => InferSchema
            case "parse" => new ParseSchema()
            case _       => new ParseSchema()
        }
    }

    private def parseFallBackMethod(value: String): FallBack = {
        sanitizeString(value) match {
            case "infer" => FallBackInfer
            case "fail"  => FallBackFail
            case "pass"  => FallBackNone
            case "raw"   => FallBackRaw
            case _       => FallBackNone
        }
    }

    def isEmpty(value: String): Boolean = {
        value.trim.isEmpty
    }
}
