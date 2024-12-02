/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.common.SparkTestBuilder
import com.github.jaime.intoParquet.exception.EnrichNotImplementedTypeException
import com.github.jaime.intoParquet.exception.NoSchemaFoundException
import com.github.jaime.intoParquet.exception.NotImplementedTypeException
import com.github.jaime.intoParquet.model.enumeration.FallBackFail
import com.github.jaime.intoParquet.model.enumeration.FallBackInfer
import com.github.jaime.intoParquet.model.enumeration.FallBackNone
import com.github.jaime.intoParquet.model.enumeration.FallBackRaw
import org.apache.spark.sql.Row
import java.sql.Date
import org.apache.spark.sql.types._
import java.sql.Timestamp

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
        assume(parse.cast.isFailure)
        assertThrows[NotImplementedTypeException](parse.cast.get)
    }

    test("Should enrich the exception with file information") {
        val parse = new Parse("wrongType", basePaths, FallBackNone)
        assume(parse.cast.isFailure)
        assertThrows[EnrichNotImplementedTypeException](parse.cast.get)
        val exception = intercept[EnrichNotImplementedTypeException](parse.cast.get)
        assertResult("wrongType")(exception.file)
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
            StructField("postal_region", IntegerType)
          )
        )
        val expectedDF = buildDataFrame(expectedData, expectedSchema)
        assertDataFrameNoOrderEquals(expectedDF, parse.readFrom)
    }

    test("Should parse all simple non-nested data types") {
        val parse = new Parse("allSimpleDataTypes", basePaths, FallBackFail)
        assume(parse.cast.isSuccess)
        val result = parse.readFrom
        val expectedData = List(
          Row(
            "example",
            true,
            Timestamp.valueOf("2024-11-10 22:01:35"),
            Date.valueOf("2024-11-27"),
            10.toByte,
            120.toByte,
            10001.toShort,
            28039.toShort,
            20240504,
            20241127,
            4147483647L,
            9147483647L,
            1.618,
            3.141592.toFloat,
            2.7182818284.toFloat,
            new java.math.BigDecimal(57.2958)
          )
        )
        val expectedSchema = StructType(
            List(
                StructField("string", StringType),
                StructField("boolean", BooleanType),
                StructField("timestamp", TimestampType),
                StructField("date", DateType),
                StructField("byte", ByteType),
                StructField("tinyint", ByteType),
                StructField("smallint", ShortType),
                StructField("short", ShortType),                
                StructField("int", IntegerType),
                StructField("integer", IntegerType),
                StructField("bigint", LongType),
                StructField("long", LongType),
                StructField("double", DoubleType),
                StructField("float", FloatType),
                StructField("real", FloatType),
                StructField("decimal", new DecimalType(8,6))                
            )
        )
        val expected = buildDataFrame(expectedData, expectedSchema)
        assertResult(expected.schema)(result.schema)
        assertDataFrameEquals(expected, result)
    }
}
