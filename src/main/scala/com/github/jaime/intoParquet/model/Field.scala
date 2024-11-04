package com.github.jaime.intoParquet.model

import com.github.jaime.intoParquet.exception.NotImplementedTypeException
import com.github.jaime.intoParquet.model.enumeration.{
    BooleanType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    SQLType,
    ShortType,
    StringType,
    TimeStampType
}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

/** Individual field description. Holds column name and type
  */
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

    override def equals(obj: Any): Boolean = {
        obj match {
            case d: Field =>
                d.fieldName.equals(this._fieldName) && d.fieldType.equals(this.fieldType)
            case _ => false
        }
    }

}

object Field {
    protected[model] def intoSQLType(value: String): SQLType = {
        val sanitizedString = sanitizeString(value)
        if (isDecimal(sanitizedString)) {
            DecimalType(sanitizedString)
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

    private def resolveCaseStatement(cleanString: String): SQLType = {
        cleanString match {
            case "string"    => StringType
            case "int"       => IntegerType
            case "boolean"   => BooleanType
            case "timestamp" => TimeStampType
            case "double"    => DoubleType
            case "bigint"    => LongType
            case "tinyint"   => ShortType
            case e           => throw new NotImplementedTypeException(e)
        }
    }
}
