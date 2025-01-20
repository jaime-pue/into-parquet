/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.app

import com.github.jaime.intoParquet.common.Resources.OutputTestFolder
import com.github.jaime.intoParquet.common.SparkTestBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class TestSparkWriter extends SparkTestBuilder {

    test("Should return a round number") {
        assertResult(1)(SparkWriter.calculateNumberOfPartitions(50000L))
    }

    test("Should ceil the number") {
        assertResult(1)(SparkWriter.calculateNumberOfPartitions(84500L))
    }

    test("Should not be zero, at least should be one") {
        assertResult(1)(SparkWriter.calculateNumberOfPartitions(1L))
    }

    test("Should not throw exception if zero rows") {
        assertResult(0)(SparkWriter.calculateNumberOfPartitions(0L))
    }

    test("Should convert one million bytes to one MB") {
        assertResult(1)(SparkWriter.ceilBytesToInt(1000000))
    }

    test("Should convert less than one million bytes to one MB") {
        assertResult(1)(SparkWriter.ceilBytesToInt(500000))
    }

    test("Should ceil 1.5m bytes to 2") {
        assertResult(2)(SparkWriter.ceilBytesToInt(1500000))
    }

    private def buildSampleData: DataFrame = {
        val Data = List(
          Row(2006, "Spirit", "Fear Dark Records", null, null, null, null, null),
          Row(2008, "Slania", "Nuclear Blast", 35, 72, null, null, null),
          Row(2009, "Evocation I - The Arcane Dominion", "Nuclear Blast", 20, 60, null, 195, null),
          Row(2010, "Everything Remains (As It Never Was)", "Nuclear Blast", 8, 19, 31, 110, 22),
          Row(2012, "Helvetios", "Nuclear Blast", 4, 27, 47, 115, 34),
          Row(2014, "Origins", "Nuclear Blast", 1, 6, 37, 70, 22),
          Row(2017, "Evocation II - Pantheon", "Nuclear Blast", 2, 11, null, 64, 18),
          Row(2019, "Ategnatos", "Nuclear Blast", 3, 11, null, 73, 12)
        )
        val schema = StructType(
            List(
                StructField("year", IntegerType),
                StructField("album", StringType),
                StructField("record_label", StringType),
                StructField("Switzerland", IntegerType),
                StructField("Germany", IntegerType),
                StructField("Finland", IntegerType),
                StructField("France", IntegerType),
                StructField("Austria", IntegerType)
            )
        )
        buildDataFrame(Data, schema).cache()
    }

    test("Should split data in a desired partition number") {
        val writer = new SparkWriter(buildSampleData, "")
        assertResult(3)(writer.setNumberOfPartitions(3).rdd.getNumPartitions)
    }

    test("Should leave one partition") {
        val writer = new SparkWriter(buildSampleData, "")
        assertResult(1)(writer.setNumberOfPartitions(1).rdd.getNumPartitions)
    }

    test("Should be at least one partition") {
        val writer = new SparkWriter(buildSampleData, "")
        assertResult(1)(writer.setNumberOfPartitions(0).rdd.getNumPartitions)
    }

    test("Should write to desired output path") {
        val writer = new SparkWriter(buildSampleData, OutputTestFolder)
        writer.writeTo()
        assertDataFrameNoOrderEquals(buildSampleData, spark.read.parquet(OutputTestFolder))
    }
}
