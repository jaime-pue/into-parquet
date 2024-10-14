package com.intoParquet.mapping

import org.scalatest.funsuite.AnyFunSuite

class TestIntoTableDescription extends AnyFunSuite {

    test("Should return only field data") {
        val example = Seq(
          "name	type	comment",
          "society_code	string	Código de sociedad"
        )
        val expected = Seq(
          "society_code	string	Código de sociedad"
        )
        assertResult(expected)(IntoTableDescription.deleteFirstLine(example))
    }

    test("Should return field data if no comment") {
        val example = Seq(
          "field_name string"
        )
        assertResult(example)(IntoTableDescription.deleteFirstLine(example))
    }

    test("Should work with regex") {
        val example = "name type comment"
        assert(IntoTableDescription.matchCase(example))
    }

    test("Should work with weird patterns") {
        val example = "name	type	comment"
        assert(IntoTableDescription.matchCase(example))
    }

    test("Should work with a field that starts with name and ends with comment") {
        val example = Seq("name string comment")
        assertResult(example)(IntoTableDescription.deleteFirstLine(example))
    }

    test("Should regex with multiple spaces") {
        val twoSpaces = "name   type  	comment"
        assert(IntoTableDescription.matchCase(twoSpaces))
    }

    test("Should work if only name and type") {
        val example = "name type"
        assert(IntoTableDescription.matchCase(example))
    }

    test("Should work if header is longer") {
        val header =
            "name	type	comment	primary_key	nullable	default_value	encoding	compression	block_size"
        assert(IntoTableDescription.matchCase(header))
    }
}
