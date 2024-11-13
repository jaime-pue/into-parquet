/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.enumeration

import org.scalatest.funsuite.AnyFunSuite

class TestCastMode extends AnyFunSuite {

    test("Should return as expected") {
        assertResult("RawSchema")(RawSchema.toString)
    }

    test("Should return infer") {
        assertResult("InferSchema")(InferSchema.toString)
    }

    test("Should return parse and add fallback information") {
        assertResult("ParseSchema with fallback: None")(new ParseSchema().toString)
    }
}
