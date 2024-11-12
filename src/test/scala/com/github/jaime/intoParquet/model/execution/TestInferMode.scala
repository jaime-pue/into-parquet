/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.common.SparkTestBuilder
import com.github.jaime.intoParquet.configuration.BasePaths
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType

class TestInferMode extends SparkTestBuilder {

    private val basePaths: BasePaths = new BasePaths(Resources.ResourceFolder)

    private def newInfer(file: String): Infer = {
        val infer        = new Infer(file, basePaths)
        infer
    }

    test("Should infer the data with timestamps") {
        val infer        = newInfer("timestampConversion")
        val TimestampSchema: StructType = StructType(
            List(
                StructField("id", IntegerType),
                StructField("name", StringType),
                StructField("start_time", TimestampType),
                StructField("end_time_with_decimals", TimestampType)
            )
        )
        assertResult(TimestampSchema)(infer.readFrom.schema)
    }

    test("Should maintain data structure with nulls") {
        val infer = newInfer("exampleTable")
        val expectedData = Seq(
            Row("John", 0),
            Row("admin", 1),
            Row("empty", null),
            Row(null, 3)
        )
        val expectedSchema = StructType(
            Seq(
                StructField("name", StringType),
                StructField("id", IntegerType)
            )
        )
        val expectedDf = buildDataFrame(expectedData, expectedSchema)
        assertDataFrameNoOrderEquals(expectedDf, infer.readFrom)
    }
}
