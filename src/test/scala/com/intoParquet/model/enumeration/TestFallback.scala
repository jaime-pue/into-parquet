package com.intoParquet.model.enumeration

import org.scalatest.funsuite.AnyFunSuite

class TestFallback extends AnyFunSuite {

    test("Should return only the suffix") {
        assertResult("raw")(FallBackRaw.toString)
    }
}
