package com.intoParquet.service

import com.intoParquet.common.Resources
import com.intoParquet.configuration.BasePaths
import org.scalatest.funsuite.AnyFunSuite

class TestFileLoader extends AnyFunSuite {
    lazy val fileLoader: FileLoader = new FileLoader(Resources.path)

    test("Should read from data/input/schema") {
        val file = fileLoader.readFile("exampleTable")
        assume(file.isDefined)
        assertResult(List("name type comment", "name string", "id int"))(file.get)
    }

    test("Should return None if no file found") {
        val file = fileLoader.readFile("imagine")
        assert(file.isEmpty)
    }

    test("Should return a list of files from raw") {
        val f = fileLoader.readAllFilesFromRaw.get
        assert(f.length > 0)
    }

    test("Should fail if base path doesn't exist") {
        val wrongDirectory: FileLoader = new FileLoader(BasePaths("Imagine"))
        val f = wrongDirectory.readAllFilesFromRaw
        assert(f.isFailure)
    }
}
