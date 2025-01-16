/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.exception.WrongFieldDescriptionException
import com.github.jaime.intoParquet.model.Field

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.matching.Regex

object IntoField extends AppLogger {

    /** Regex looks for the pattern `FieldName Type`. If it follows a pair of parenthesis and within
     * them `(digit, digit)`, it signals it may face a decimal type that can have several spaces in
     * between
     */
    private val regex: Regex = raw"(\w+)\s+([a-zA-Z]+(\s?\(\d+,\s*\d+\))?).*".r

    def tryFromDescription(description: String): Try[Field] = {
        logDebug(s"Cast from <<$description>>")
        val e = trySplitValue(description) match {
            case Failure(exception) => return Failure(exception)
            case Success(value)     => value
        }
        val dataType = IntoSQLDataType.tryFromString(e(1)) match {
            case Failure(exception) => return Failure(exception)
            case Success(value)     => value
        }
        Success(new Field(e(0), dataType))
    }

    def trySplitValue(line: String): Try[Array[String]] = {
        line match {
            case regex(first, second, _*) => Success(Array(first, second))
            case _                        => Failure(new WrongFieldDescriptionException(line))
        }
    }
}
