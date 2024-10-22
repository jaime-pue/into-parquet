package com.github.jaime.intoParquet.model

import com.github.jaime.intoParquet.exception.NotImplementedTypeException
import com.github.jaime.intoParquet.model.enumeration.DecimalType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, types}
import org.scalatest.funsuite.AnyFunSuite

class TestField extends AnyFunSuite {

    test("Should throw exception when not recognized") {
        val value = "random"
        assertThrows[NotImplementedTypeException](Field.intoSQLType(value))
    }

    test("Should work with decimal types") {
        assert(Field.intoSQLType("decimal(38,2)").isInstanceOf[DecimalType])
        assert(Field.intoSQLType("decimal(38,2)").value.isInstanceOf[types.DecimalType])
    }

    test("Should lowercase string") {
        assertResult("string")(Field.sanitizeString("STRING"))
    }

    test("Should trim string value") {
        assertResult("string")(Field.sanitizeString(" string "))
    }

    test("Should work with capital values and different white spaces") {
        assertResult("int")(Field.sanitizeString(" Int"))
    }

    test("Should be equal") {
        val a = new Field("a", "string")
        val expected = new Field("a", "string")
        assert(a.equals(expected))
    }

    test("Should not be equal") {
        val c = new Field("c", "string")
        val b = new Field("a", "int")
        val expected = new Field("a", "string")
        assert(!c.equals(expected))
        assert(!b.equals(expected))
    }

    test("Should conform a column expression") {
        val a = new Field("a", "boolean")
        val expected = col("a").cast(types.BooleanType)
        assume(a.colExpression.isInstanceOf[Column])
        assertResult(expected)(a.colExpression)
    }

    test("Should be correct type") {
        val b = new Field("b", "double")
        assert(b.fieldType.isInstanceOf[types.DoubleType])
    }
}
