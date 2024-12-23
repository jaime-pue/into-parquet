/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.model.Field
import com.github.jaime.intoParquet.model.enumeration.StringDataType
import org.scalatest.funsuite.AnyFunSuite

class TestIntoField extends AnyFunSuite {

 test("Should split in desired items") {
    val sampleField: String = "society_code	string	Código de sociedad"
    val result: Array[String] = Array("society_code", "string")
    assertResult(result)(IntoField.splitValue(sampleField))
 }

    test("Should create field with only name and type") {
        val sample = "field_name string"
        val result = Array("field_name", "string")
        assertResult(result)(IntoField.splitValue(sample))
    }

    test("Should create a new field") {
        val sample = "field_name string"
        val expected = new Field("field_name", StringDataType)
        assertResult(expected)(IntoField.fromDescription(sample))
    }
}
