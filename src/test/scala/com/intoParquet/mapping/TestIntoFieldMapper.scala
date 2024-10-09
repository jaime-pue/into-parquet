package com.intoParquet.mapping

import com.intoParquet.model.Field
import org.scalatest.funsuite.AnyFunSuite

class TestIntoFieldMapper extends AnyFunSuite {

 test("Should split in desired items") {
    val sampleField: String = "society_code	string	Código de sociedad"
    val result: Array[String] = Array("society_code", "string")
    assertResult(result)(IntoFieldMapper.splitValue(sampleField))
 }

    test("Should create field with only name and type") {
        val sample = "field_name string"
        val result = Array("field_name", "string")
        assertResult(result)(IntoFieldMapper.splitValue(sample))
    }

    test("Should create a new field") {
        val sample = "field_name string"
        val expected = new Field("field_name", "string")
        assertResult(expected)(IntoFieldMapper.fromDescription(sample))
    }
}
