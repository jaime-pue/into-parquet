/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model

import com.github.jaime.intoParquet.model.enumeration.BooleanDataType
import com.github.jaime.intoParquet.model.enumeration.DoubleDataType
import com.github.jaime.intoParquet.model.enumeration.IntegerDataType
import com.github.jaime.intoParquet.model.enumeration.StringDataType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Column
import org.apache.spark.sql.types
import org.scalatest.funsuite.AnyFunSuite

class TestField extends AnyFunSuite {

    test("Should be equal") {
        val a = new Field("a", StringDataType)
        val expected = new Field("a", StringDataType)
        assert(a.equals(expected))
    }

    test("Should not be equal") {
        val c = new Field("c", StringDataType)
        val b = new Field("a", IntegerDataType)
        val expected = new Field("a", StringDataType)
        assert(!c.equals(expected))
        assert(!b.equals(expected))
    }

    test("Should not be equal if different Type") {
        val a = new Field("a", StringDataType)
        assert(!a.equals("HELLO"))
    }

    test("Should conform a column expression") {
        val a = new Field("a", BooleanDataType)
        val expected = col("a").cast(types.BooleanType)
        assume(a.colExpression.isInstanceOf[Column])
        assertResult(expected)(a.colExpression)
    }

    test("Should be correct type") {
        val b = new Field("b", DoubleDataType)
        assert(b.fieldType.isInstanceOf[types.DoubleType])
    }
}
