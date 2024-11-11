/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.exception.NotImplementedTypeException
import com.github.jaime.intoParquet.model.enumeration.BooleanDataType
import com.github.jaime.intoParquet.model.enumeration.DecimalDataType
import com.github.jaime.intoParquet.model.enumeration.DoubleDataType
import com.github.jaime.intoParquet.model.enumeration.IntegerDataType
import com.github.jaime.intoParquet.model.enumeration.LongDataType
import com.github.jaime.intoParquet.model.enumeration.SQLDataType
import com.github.jaime.intoParquet.model.enumeration.ShortDataType
import com.github.jaime.intoParquet.model.enumeration.StringDataType
import com.github.jaime.intoParquet.model.enumeration.TimeStampDataType

object IntoSQLDataType {

    def mapFrom(value: String): SQLDataType = {
        val sanitizedString = sanitizeString(value)
        if (isDecimal(sanitizedString)) {
            DecimalDataType(sanitizedString)
        } else {
            resolveCaseStatement(sanitizedString)
        }
    }

    protected[mapping] def sanitizeString(value: String): String = {
        value.trim().toLowerCase()
    }

    private def isDecimal(value: String): Boolean = {
        value.startsWith("decimal")
    }

    private def resolveCaseStatement(cleanString: String): SQLDataType = {
        cleanString match {
            case "string"    => StringDataType
            case "int"       => IntegerDataType
            case "boolean"   => BooleanDataType
            case "timestamp" => TimeStampDataType
            case "double"    => DoubleDataType
            case "bigint"    => LongDataType
            case "tinyint"   => ShortDataType
            case e           => throw new NotImplementedTypeException(e)
        }
    }
}
