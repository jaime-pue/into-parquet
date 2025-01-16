/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.enumeration

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types

sealed trait SQLDataType {
    val value: DataType

    override def toString: String = getClass.getSimpleName.replace("$", "")
}

object StringDataType extends SQLDataType {
    override val value: DataType = types.StringType
}

object BooleanDataType extends SQLDataType {

    override val value: DataType = types.BooleanType

}

object IntegerDataType extends SQLDataType {
    override val value: DataType = types.IntegerType
}

object TimeStampDataType extends SQLDataType {
    override val value: DataType = types.TimestampType
}

object DoubleDataType extends SQLDataType {
    override val value: DataType = types.DoubleType
}

object LongDataType extends SQLDataType {
    override val value: DataType = types.LongType
}

class DecimalDataType(val precision: Int, val scale: Int) extends SQLDataType {

    override val value: DataType = types.DecimalType(precision, scale)

    override def equals(obj: Any): Boolean = {
        obj match {
            case d: DecimalDataType =>
                d.value.equals(this.value)
            case _ => false
        }
    }
}

object DecimalDataType {
    protected[enumeration] def fromString(value: String): List[Int] = {
        value.filter(c => c.isDigit || c.equals(',')).split(",").map(i => i.toInt).toList
    }

    def apply(value: String): DecimalDataType = {
        val values = fromString(value)
        new DecimalDataType(values.head, values.tail.head)
    }
}

object ShortDataType extends SQLDataType {

    override val value: DataType = types.ShortType

}

object DateDataType extends SQLDataType {

    override val value: DataType = types.DateType
}

object FloatDataType extends SQLDataType {

    override val value: DataType = types.FloatType
}

object ByteDataType extends SQLDataType {
    override val value: DataType = types.ByteType
}