/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.enumeration

import org.scalatest.funsuite.AnyFunSuite

class TestFallback extends AnyFunSuite {

    test("Should return only the suffix") {
        assertResult("Raw")(FallBackRaw.toString)
    }
}
