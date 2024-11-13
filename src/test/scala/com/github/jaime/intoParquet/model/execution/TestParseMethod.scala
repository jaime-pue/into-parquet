/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.common.SparkTestBuilder
import com.github.jaime.intoParquet.exception.NoSchemaFoundException
import com.github.jaime.intoParquet.exception.NotImplementedTypeException
import com.github.jaime.intoParquet.model.enumeration.FallBackFail
import com.github.jaime.intoParquet.model.enumeration.FallBackInfer
import com.github.jaime.intoParquet.model.enumeration.FallBackNone
import com.github.jaime.intoParquet.model.enumeration.FallBackRaw
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

class TestParseMethod extends SparkTestBuilder {

    private val basePaths = Resources.path

    test("Should do noting as fallback") {
        val parse = new Parse("timestampConversion", basePaths, FallBackNone)
        assert(parse.cast.isSuccess)
    }

    test("Should return a failure") {
        val parse = new Parse("timestampConversion", basePaths, FallBackFail)
        val e     = parse.cast
        assert(e.isFailure)
        assertThrows[NoSchemaFoundException](e.get)
    }

    test("Should parse with infer") {
        val parse = new Parse("timestampConversion", basePaths, FallBackInfer)
        assert(parse.cast.isSuccess)
    }

    test("Should parse with raw") {
        val parse = new Parse("timestampConversion", basePaths, FallBackRaw)
        assert(parse.cast.isSuccess)
    }

    test("Should finish Ok if everything is right") {
        val parse = new Parse("exampleTable", basePaths, FallBackNone)
        assert(parse.cast.isSuccess)
    }

    test("Should be a failure if table description has wrong format but exists") {
        val parse = new Parse("wrongType", basePaths, FallBackNone)
        assert(parse.cast.isFailure)
        assertThrows[NotImplementedTypeException](parse.cast.get)
    }

    test(
      "Should not fail if the schema is wrong and is trying to force a conversion for a different type"
    ) {
        val parse = new Parse("badSchema", basePaths, FallBackNone)
        assume(parse.cast.isSuccess)
        val expectedData = List(
          Row(1, "none", null),
          Row(2, "john", 2345),
          Row(3, "mary", null),
          Row(4, "carrot", 1002)
        )
        val expectedSchema = StructType(
          List(
            StructField("id", IntegerType),
            StructField("name", StringType),
            StructField("postal_region", IntegerType),
          )
        )
        val expectedDF = buildDataFrame(expectedData, expectedSchema)
        assertDataFrameNoOrderEquals(expectedDF, parse.readFrom)
    }
}
