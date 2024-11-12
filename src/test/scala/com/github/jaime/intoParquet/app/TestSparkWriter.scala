/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.app

import org.scalatest.funsuite.AnyFunSuite

class TestSparkWriter extends AnyFunSuite {

    test("Should return a round number") {
        assertResult(1)(SparkWriter.calculateNumberOfPartitions(50000L))
    }

    test("Should ceil the number") {
        assertResult(2)(SparkWriter.calculateNumberOfPartitions(84500L))
    }

    test("Should not be zero, at least should be one") {
        assertResult(1)(SparkWriter.calculateNumberOfPartitions(1L))
    }

    test("Should not throw exception if zero rows") {
        assertResult(0)(SparkWriter.calculateNumberOfPartitions(0L))
    }
}
