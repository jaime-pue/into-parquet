package com.intoParquet.utils

import org.scalatest.funsuite.AnyFunSuite

class TestParser extends AnyFunSuite {

    test("Should parse a string") {
        val input = Array("--files", "random")
        val args  = Parser.parseSystemArgs(input)
        assertResult("random")(args.get.csvFile.get)
    }

    test("Should declare empty as None") {
        val input = Array("-f", "")
        val args = Parser.parseSystemArgs(input)
        assert(args.get.csvFile.isEmpty)
    }

    test("Should set recursive as true if files are empty") {
        val input = Array("-f", "")
        val args = Parser.parseSystemArgs(input)
        assume(args.get.csvFile.isEmpty)
        assert(args.get.recursive)
    }

    test("Should declare empty as None even with multiple whitespaces") {
        val input = Array("-f", "    ")
        val args = Parser.parseSystemArgs(input)
        assert(args.get.csvFile.isEmpty)
    }

    test("Should work if nothing is passed") {
        val input: Array[String] = Array()
        val args                 = Parser.parseSystemArgs(input)
        assert(args.get.csvFile.isEmpty)
    }

    test("Should work with simple call") {
        val input = Array("-f", "random")
        val args  = Parser.parseSystemArgs(input)
        assertResult("random")(args.get.csvFile.get)
    }

    test("Should fail if wrong inputs") {
        val input = Array("-i", "2")
        val args  = Parser.parseSystemArgs(input)
        assert(args.isEmpty)
    }

    test("Should default to recursive method") {
        val input: Array[String] = Array()
        val args = Parser.parseSystemArgs(input).get
        assert(args.recursive)
    }

    test("Should force recursive to false if files found") {
        val input = Array("-f", "Any")
        val args = Parser.parseSystemArgs(input).get
        assume(args.csvFile.isDefined)
        assert(!args.recursive)
    }

    test("Should fail if write mode is not allowed") {
        val input = Array("-m", "blah")
        val args = Parser.parseSystemArgs(input)
        assert(args.isEmpty)
    }

    ignore("Should display version info") {
        val input = Array("--version")
        assert(Parser.parseSystemArgs(input).isDefined)
    }

    ignore("Should display help info") {
        val input = Array("--help")
        assert(Parser.parseSystemArgs(input).isDefined)
    }

}
