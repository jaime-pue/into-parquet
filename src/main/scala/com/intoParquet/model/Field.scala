package com.intoParquet.model

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

class Field(_fieldName: String, _fieldType: SQLType) {
    private val fieldNameHolder: String  = _fieldName
    private val fieldTypeHolder: SQLType = _fieldType

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

}

object Field {
    protected[model] def intoSQLType(value: String): SQLType = {
        sanitizeString(value) match {
            case "string"        => StringType
            case "integer"       => IntegerType
            case "boolean"       => BooleanType
            case "timestamp"     => TimeStampType
            case "double"        => DoubleType
            case "bigint"        => LongType
            case "decimal(38,2)" => new DecimalType(38, 2)
            case "tinyint"       => ShortType
            case e               => throw new Exception(s"Not recognized type conversion for $e")
        }
    }

    private def sanitizeString(value: String): String = {
        value.trim().toLowerCase()
    }
}
