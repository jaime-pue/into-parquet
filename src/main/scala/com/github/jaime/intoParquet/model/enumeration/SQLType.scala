package com.github.jaime.intoParquet.model.enumeration

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types

sealed trait SQLType {
    val value: DataType
}

object StringType extends SQLType {
    override val value: DataType = types.StringType
}

object BooleanType extends SQLType {

    override val value: DataType = types.BooleanType

}

object IntegerType extends SQLType {
    override val value: DataType = types.IntegerType
}

object TimeStampType extends SQLType {
    override val value: DataType = types.TimestampType
}

object DoubleType extends SQLType {
    override val value: DataType = types.DoubleType
}

object LongType extends SQLType {
    override val value: DataType = types.LongType
}

class DecimalType(precision: Int, scale: Int) extends SQLType {

    override val value: DataType = types.DecimalType(precision, scale)

}

object ShortType extends SQLType {

  override val value: DataType = types.ShortType


}

