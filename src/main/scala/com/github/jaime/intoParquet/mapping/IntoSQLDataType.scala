/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.exception.NotImplementedTypeException
import com.github.jaime.intoParquet.model.enumeration._

object IntoSQLDataType {

    private val PunctuationSigns = "[,;.:]"

    def mapFrom(value: String): SQLDataType = {
        val sanitizedString = sanitizeString(value)
        if (isDecimal(sanitizedString)) {
            DecimalDataType(sanitizedString)
        } else {
            resolveCaseStatement(sanitizedString)
        }
    }

    protected[mapping] def sanitizeString(value: String): String = {
        val raw = value.trim().toLowerCase()
        if (hasPunctuationSign(raw)) {
            raw.dropRight(1)
        } else {
            raw
        }
    }

    private def hasPunctuationSign(stringValue: String): Boolean = {
        PunctuationSigns.contains(stringValue.reverse.head)
    }

    private def isDecimal(value: String): Boolean = {
        value.startsWith("decimal")
    }

    private def resolveCaseStatement(cleanString: String): SQLDataType = {
        cleanString match {
            case "string"  => StringDataType
            case "boolean" => BooleanDataType
            // date types
            case "timestamp" => TimeStampDataType
            case "date"      => DateDataType
            // numeric types
            case "byte"     => ByteDataType
            case "tinyint"  => ByteDataType
            case "smallint" => ShortDataType
            case "short"    => ShortDataType
            case "int"      => IntegerDataType
            case "integer"  => IntegerDataType
            case "bigint"   => LongDataType
            case "long"     => LongDataType
            // decimal types
            case "double" => DoubleDataType
            case "float"  => FloatDataType
            case "real"   => FloatDataType
            case e        => throw new NotImplementedTypeException(e)
        }
    }
}
