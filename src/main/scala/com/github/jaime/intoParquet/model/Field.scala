/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model

import com.github.jaime.intoParquet.exception.NotImplementedTypeException
import com.github.jaime.intoParquet.model.enumeration.{
    BooleanDataType,
    DecimalDataType,
    DoubleDataType,
    IntegerDataType,
    LongDataType,
    SQLDataType,
    ShortDataType,
    StringDataType,
    TimeStampDataType
}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

/** Individual field description. Holds column name and type
  */
class Field(_fieldName: String, _fieldType: SQLDataType) {
    private val fieldNameHolder: String  = _fieldName
    private val fieldTypeHolder: SQLDataType = _fieldType

    def fieldType: DataType = {
        this.fieldTypeHolder.value
    }

    def fieldName: String = fieldNameHolder

    def colExpression: Column = {
        col(this.fieldNameHolder).cast(this.fieldType)
    }

    def this(_fieldName: String, _fieldType: String) = {
        this(_fieldName, Field.intoSQLType(_fieldType))
    }

    override def equals(obj: Any): Boolean = {
        obj match {
            case d: Field =>
                d.fieldName.equals(this._fieldName) && d.fieldType.equals(this.fieldType)
            case _ => false
        }
    }

}

object Field {
    protected[model] def intoSQLType(value: String): SQLDataType = {
        val sanitizedString = sanitizeString(value)
        if (isDecimal(sanitizedString)) {
            DecimalDataType(sanitizedString)
        } else {
            resolveCaseStatement(sanitizedString)
        }
    }

    protected[model] def sanitizeString(value: String): String = {
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
