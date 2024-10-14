package com.intoParquet.utils

import org.scalatest.funsuite.AnyFunSuite
import com.intoParquet.model.enumeration.{InferSchema, Raw, ReadSchema, WriteMode}

class TestParser extends AnyFunSuite {

    test("Should parse a string") {
        val input = Array("--files", "random")
        val args  = Parser.parseSystemArgs(input)
        assertResult("random")(args.get.csvFile.get)
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

    test("Should list files recursively") {
        val input = Array("-R")
        val args  = Parser.parseSystemArgs(input).get
        assert(args.recursive)
    }

    test("Should accept combinations") {
        val input = Array("-R", "-f", "random", "--infer")
        val args  = Parser.parseSystemArgs(input).get
        assert(args.recursive)
        assert(args.writeMethod.equals(InferSchema))
        assert(args.csvFile.get.equals("random"))
    }
}
