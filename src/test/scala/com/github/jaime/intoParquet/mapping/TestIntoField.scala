/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.model.Field
import com.github.jaime.intoParquet.model.enumeration.DecimalDataType
import com.github.jaime.intoParquet.model.enumeration.StringDataType
import org.scalatest.funsuite.AnyFunSuite

class TestIntoField extends AnyFunSuite {

 test("Should split in desired items") {
    val sampleField: String = "society_code	string	Código de sociedad"
    val result: Array[String] = Array("society_code", "string")
    assertResult(result)(IntoField.trySplitValue(sampleField).get)
 }

    test("Should create field with only name and type") {
        val sample = "field_name string"
        val result = Array("field_name", "string")
        assertResult(result)(IntoField.trySplitValue(sample).get)
    }

    test("Should create a new field") {
        val sample = "field_name string"
        val expected = new Field("field_name", StringDataType)
        assertResult(expected)(IntoField.tryFromDescription(sample).get)
    }

    test("Should split decimal in an appropriate way") {
        val sample = "field decimal(3,1)"
        val expected = new Field("field", new DecimalDataType(3, 1))
        assertResult(expected)(IntoField.tryFromDescription(sample).get)
    }

    test("Should split decimal even if spacing") {
        val sample = "field decimal(3, 1)"
        val expected = new Field("field", new DecimalDataType(3, 1))
        assertResult(expected)(IntoField.tryFromDescription(sample).get)
    }

    test("Should split values") {
        val sample = "field type comment"
        val expected = List("field", "type")
        assertResult(expected)(IntoField.trySplitValue(sample).get)
    }

    test("Should split values with ()") {
        val sample = "field type() comment"
        val expected = List("field", "type")
        assertResult(expected)(IntoField.trySplitValue(sample).get)
    }

    test("Should split values with () and digits") {
        val sample = "field type(2,4) comment"
        val expected = List("field", "type(2,4)")
        assertResult(expected)(IntoField.trySplitValue(sample).get)
    }

    test("Should split values with (), digits and spacing") {
        val sample = "field type(2, 4) comment"
        val expected = List("field", "type(2, 4)")
        assertResult(expected)(IntoField.trySplitValue(sample).get)
    }
    test("Should split values with (), digits and multiple spacing") {
        val sample = "field type(2,   4) comment"
        val expected = List("field", "type(2,   4)")
        assertResult(expected)(IntoField.trySplitValue(sample).get)
    }

    test("Should throw an exception if no matches") {
        val sample = "aaa"
        assertThrows[Exception](IntoField.trySplitValue(sample).get)
    }

    test("Should work if there is a space between the type") {
        val sample = "random type (3,2)"
        val expected = List("random", "type (3,2)")
        assertResult(expected)(IntoField.trySplitValue(sample).get)
    }
}
