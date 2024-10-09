package com.intoParquet.mapping

import org.apache.spark.sql.DataFrame
import com.intoParquet.model.FieldDescriptors
import com.intoParquet.model.Field

object IntoColumnMapper {

    def applySchema(df: DataFrame, description: FieldDescriptors): DataFrame = {
        description.fields.foldLeft(df) { (temp, field) =>
            temp.withColumn(field.fieldName, field.colExpression)
        }
    }
}
