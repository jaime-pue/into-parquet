/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.service

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.exception.NoFileFoundException
import com.github.jaime.intoParquet.service.FileLoader.filesExists
import com.github.jaime.intoParquet.service.FileLoader.readAllFilesFromRaw
import org.scalatest.funsuite.AnyFunSuite
import com.github.jaime.intoParquet.service.FileLoader.readFile

class TestFileLoader extends AnyFunSuite {

    private val inputPath = Resources.path.inputBasePath

    test("Should read from data/input/schema") {
        val file = readFile(inputPath, "exampleTable")
        assume(file.isDefined)
        assertResult(List("name type comment", "name string", "id int"))(file.get)
    }

    test("Should return None if no file found") {
        val file = readFile(inputPath, "imagine")
        assert(file.isEmpty)
    }

    test("Should return a list of files from raw") {
        val f = readAllFilesFromRaw(inputPath)
        assert(f.length > 0)
    }

    test("Should fail if inputDir path doesn't exist") {
        val wrongDirectory = new BasePaths("Imagine").inputBasePath
        assertThrows[NoFileFoundException](readAllFilesFromRaw(wrongDirectory))
    }

    test("Should not fail if no csv files found") {
        val wrongDirectory = new BasePaths(Some(Resources.ResourceFolder)).inputBasePath
        assertResult(Array[String]())(readAllFilesFromRaw(wrongDirectory))
    }

    test("Should return all existing csv files") {
        val files   = Array("exampleTable", "random")
        val outcome = filesExists(inputPath, files)
        assertResult(Array("exampleTable"))(outcome)
    }

    test("Should return an empty array if no file coincidence") {
        val files = Array("E", "A")
        assertResult(Array[String]())(filesExists(inputPath, files))
    }
}
