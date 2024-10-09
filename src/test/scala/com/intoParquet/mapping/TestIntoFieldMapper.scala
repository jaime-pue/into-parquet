package com.intoParquet.mapping

import org.scalatest.funsuite.AnyFunSuite

class TestIntoFieldMapper extends AnyFunSuite {

 test("Should split in desired items") {
    val sampleField: String = "society_code	string	Código de sociedad"
    val result: Array[String] = Array("society_code", "string")
    assertResult(result)(IntoFieldMapper.splitValue(sampleField))
 } 
}
