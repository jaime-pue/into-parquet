/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model

import com.github.jaime.intoParquet.mapping.IntoField
import com.github.jaime.intoParquet.model.enumeration.SQLDataType
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataType

import scala.util.Try

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

    override def equals(obj: Any): Boolean = {
        obj match {
            case d: Field =>
                d.fieldName.equals(this._fieldName) && d.fieldType.equals(this.fieldType)
            case _ => false
        }
    }

    override def toString: String = {
        s"Field: $fieldNameHolder >> ${fieldTypeHolder.toString}"
    }

}

object Field {
    def fromDescription(value: String): Try[Field] = {
        IntoField.tryFromDescription(value)
    }
}