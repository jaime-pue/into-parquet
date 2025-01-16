/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.enumeration

import org.scalatest.funsuite.AnyFunSuite

class TestDecimalType extends AnyFunSuite {

    private def composeDecimal(valOne: Int, valTwo: Int): String = {
        s"decimal($valOne,$valTwo)"
    }

    test("Should work with a string constructor") {
        val a = DecimalDataType.fromString(composeDecimal(8, 2))
        assertResult((8, 2))((a(0), a(1)))
    }

    test("Should build a new class") {
        val a = DecimalDataType(composeDecimal(38, 4))
        assertResult(38)(a.precision)
        assertResult(4)(a.scale)
    }

    test("Should work with different spacing") {
        assertResult(List(10, 4))(DecimalDataType.fromString("decimal(10, 4)"))
    }

    test("Should work with a single space between decimal and parenthesis") {
        assertResult(List(3, 2))(DecimalDataType.fromString("decimal (3,2"))
    }
}
