/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.service

import com.github.jaime.intoParquet.model.enumeration.FallBackFail
import com.github.jaime.intoParquet.model.enumeration.FallBackInfer
import com.github.jaime.intoParquet.model.enumeration.ParseSchema
import com.github.jaime.intoParquet.model.enumeration.RawSchema
import org.scalatest.funsuite.AnyFunSuite

class TestParser extends AnyFunSuite {

    test("Should parse a string") {
        val input = Array("--files", "random")
        val args  = Parser.parseSystemArgs(input)
        assertResult("random")(args.get.csvFile.get)
    }

    test("Should declare empty as None") {
        val input = Array("-f", "")
        val args  = Parser.parseSystemArgs(input)
        assert(args.get.csvFile.isEmpty)
    }

    test("Should declare empty as None even with multiple whitespaces") {
        val input = Array("-f", "    ")
        val args  = Parser.parseSystemArgs(input)
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

    test("Should fail if write mode is not allowed") {
        val input = Array("-m", "blah")
        val args  = Parser.parseSystemArgs(input)
        assert(args.isEmpty)
    }

    test("Should parse to another parser method") {
        val input = Array("-m", "raw")
        val args = Parser.parseSystemArgs(input)
        assertResult(RawSchema)(args.get.castMethod.get)
    }

    ignore("Should display version info") {
        val input = Array("--version")
        assert(Parser.parseSystemArgs(input).isDefined)
    }

    ignore("Should display help info") {
        val input = Array("--help")
        assert(Parser.parseSystemArgs(input).isDefined)
    }

    test("Should parse fallback") {
        val input = Array("-fb", "fail")
        val args  = Parser.parseSystemArgs(input).get
        assertResult(FallBackFail)(args.fallBack.get)
    }

    // This test should be done later
    ignore("Should fail if using a different mode other than parse schema") {
        val input = Array("-m", "raw", "-fb", "fail")
        val args  = Parser.parseSystemArgs(input)
        assert(args.isEmpty)
    }

    test("Should work if parse schema and a fallback mode") {
        val input = Array("--fallback", "infer", "--mode", "parse")
        val args = Parser.parseSystemArgs(input)
        assume(args.isDefined)
        assert(args.get.castMethod.get.isInstanceOf[ParseSchema])
        assert(args.get.fallBack.isDefined)
        assertResult(FallBackInfer)(args.get.fallBack.get)
    }

    test("Should activate debug mode flag") {
        val input = Array("--debug")
        val args = Parser.parseSystemArgs(input)
        assume(args.isDefined)
        assert(args.get.debugMode)
    }

    test("Should exclude some files") {
        val input = Array("--exclude", "one,two")
        val args = Parser.parseSystemArgs(input)
        assume(args.isDefined)
        assume(args.get.excludeCSV.isDefined)
        assertResult("one,two")(args.get.excludeCSV.get)
    }

    test("Should return None if no excluded line") {
        val args = Parser.parseSystemArgs(Array[String]())
        assume(args.isDefined)
        assert(args.get.excludeCSV.isEmpty)
    }
}
