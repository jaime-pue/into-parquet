/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.utils

import com.github.jaime.intoParquet.model.enumeration.{FallBackFail, FallBackInfer, ParseSchema, RawSchema}
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

    test("Should set recursive as true if files are empty") {
        val input = Array("-f", "")
        val args  = Parser.parseSystemArgs(input)
        assume(args.get.csvFile.isEmpty)
        assert(args.get.recursive)
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

    test("Should default to recursive method") {
        val input: Array[String] = Array()
        val args                 = Parser.parseSystemArgs(input).get
        assert(args.recursive)
    }

    test("Should force recursive to false if files found") {
        val input = Array("-f", "Any")
        val args  = Parser.parseSystemArgs(input).get
        assume(args.csvFile.isDefined)
        assert(!args.recursive)
    }

    test("Should fail if write mode is not allowed") {
        val input = Array("-m", "blah")
        val args  = Parser.parseSystemArgs(input)
        assert(args.isEmpty)
    }

    test("Should parse to another parser method") {
        val input = Array("-m", "raw")
        val args = Parser.parseSystemArgs(input)
        assertResult(RawSchema)(args.get.castMethod)
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
        assert(args.castMethod.isInstanceOf[ParseSchema])
        assertResult(FallBackFail)(args.fallBack.get)
    }

    test("Should fail if using a different mode other than parse schema") {
        val input = Array("-m", "raw", "-fb", "fail")
        val args  = Parser.parseSystemArgs(input)
        assert(args.isEmpty)
    }

    test("Should work if parse schema and a fallback mode") {
        val input = Array("--fallback", "infer", "--mode", "parse")
        val args = Parser.parseSystemArgs(input)
        assume(args.isDefined)
        assert(args.get.castMethod.isInstanceOf[ParseSchema])
        assert(args.get.fallBack.isDefined)
        assertResult(FallBackInfer)(args.get.fallBack.get)
    }
}
