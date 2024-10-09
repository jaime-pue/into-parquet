package com.intoParquet.model

import org.scalatest.funsuite.AnyFunSuite

class TestTableDescription extends AnyFunSuite {

    test("Should turn a string into a seq") {
        val example = """
        name	type	comment
        society_code	string	Código de sociedad
        """
        val expected = Seq(
          "name	type	comment",
          "society_code	string	Código de sociedad",

        )
        assertResult(expected)(TableDescription.splitInLines(example))
    }

    test("Should return only field data") {
        val example = Seq(
          "name	type	comment",
          "society_code	string	Código de sociedad",
        )
        val expected = Seq(
          "society_code	string	Código de sociedad",
        )
        assertResult(expected)(TableDescription.deleteFirstLine(example))
    }

    test("Should return field data if no comment") {
        val example = Seq(
            "field_name string"
        )
        assertResult(example)(TableDescription.deleteFirstLine(example))
    }

    test("Should work with regex") {
        val example = "name type comment"
        assert(TableDescription.matchCase(example))
    }

    test("Should work with weird patterns") {
        val example = "name	type	comment"
        assert(TableDescription.matchCase(example))
    }

    test("Should work with a field that starts with name and ends with comment") {
        val example = Seq("name string comment")
        assertResult(example)(TableDescription.deleteFirstLine(example))
    }

    test("Should regex with multiple spaces") {
        val twoSpaces = "name   type  	comment"
        assert(TableDescription.matchCase(twoSpaces))
    }

    test("Should work if only name and type") {
        val example = "name type"
        assert(TableDescription.matchCase(example))
    }
}
