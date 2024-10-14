package com.intoParquet.service

import com.intoParquet.exception.NoFileFoundException
import org.scalatest.funsuite.AnyFunSuite

class TestFileLoader extends AnyFunSuite {
    test("Should read from data/input/schema") {
        val file = FileLoader.readFile("example")
        assume(file.isDefined)
        assertResult(List("type name", "id int"))(file.get)
    }

    test("Should return None if no file found") {
        val file = FileLoader.readFile("imagine")
        assert(file.isEmpty)
    }
}
