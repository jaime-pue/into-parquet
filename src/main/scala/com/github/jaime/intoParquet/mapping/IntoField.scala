/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.model.Field

import scala.util.matching.Regex

object IntoField extends AppLogger {

    def fromDescription(description: String): Field = {
        logDebug(s"Cast from $description")
        val e = splitValue(description)
        val dataType = IntoSQLDataType.mapFrom(e(1))
        new Field(e(0), dataType)
    }

    /**Regex looks for the pattern field name space type followed by a parenthesis
     * which indicates it may face a decimal type that can have several spaces in between*/
    protected[mapping] def splitValue(line: String): Array[String] = {
        val regex: Regex = raw"(\w+)\s+([a-zA-Z]+\(\d+,\s*\d+\)).*".r
        line match {
            case regex(first, second) => Array(first, second)
            case _ => line.split("\\s").take(2)
        }
    }
}
