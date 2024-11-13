/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.exception.NotImplementedTypeException
import com.github.jaime.intoParquet.model.enumeration.DecimalDataType
import com.github.jaime.intoParquet.model.enumeration.IntegerDataType
import com.github.jaime.intoParquet.model.enumeration.StringDataType
import org.apache.spark.sql.types
import org.scalatest.funsuite.AnyFunSuite

class TestIntoSQLDataType extends AnyFunSuite{


    test("Should throw exception when not recognized") {
        val value = "random"
        assertThrows[NotImplementedTypeException](IntoSQLDataType.mapFrom(value))
    }

    test("Should work with decimal types") {
        assert(IntoSQLDataType.mapFrom("decimal(38,2)").isInstanceOf[DecimalDataType])
        assert(IntoSQLDataType.mapFrom("decimal(38,2)").value.isInstanceOf[types.DecimalType])
    }

    test("Should work with a generic decimal type") {
        assert(IntoSQLDataType.mapFrom("decimal(8,2)").isInstanceOf[DecimalDataType])
        assert(IntoSQLDataType.mapFrom("decimal(8,2)").value.isInstanceOf[types.DecimalType])
    }

    test("Should keep precision and scale values") {
        val t = IntoSQLDataType.mapFrom("decimal(38,2)")
        assume(t.isInstanceOf[DecimalDataType])
        val casted = t.asInstanceOf[DecimalDataType]
        assertResult(38)(casted.precision)
        assertResult(2)(casted.scale)
    }

    test("Should keep precision and scale values, round #2") {
        val t = IntoSQLDataType.mapFrom("decimal(8,4)")
        assume(t.isInstanceOf[DecimalDataType])
        val casted = t.asInstanceOf[DecimalDataType]
        assertResult(8)(casted.precision)
        assertResult(4)(casted.scale)
    }

    test("Should lowercase string") {
        assertResult("string")(IntoSQLDataType.sanitizeString("STRING"))
    }

    test("Should trim string value") {
        assertResult("string")(IntoSQLDataType.sanitizeString(" string "))
    }

    test("Should work with capital values and different white spaces") {
        assertResult("int")(IntoSQLDataType.sanitizeString(" Int"))
    }

    test("Should clear commas") {
        assertResult(IntegerDataType)(IntoSQLDataType.mapFrom("int,"))
    }

    test("Should clear punctuation signs") {
        assertResult(StringDataType)(IntoSQLDataType.mapFrom("string;"))
    }

}
