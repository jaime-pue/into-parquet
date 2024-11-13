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

    private val path = Resources.path

    test("Should read from data/input/exampleTable") {
        val file = readFile(path.absoluteInputTableDescriptionPath("exampleTable"))
        assume(file.isDefined)
        assertResult(List("name type comment", "name string", "id int"))(file.get)
    }

    test("Should return None if no file found") {
        val file = readFile(path.absoluteInputTableDescriptionPath("imagine"))
        assert(file.isEmpty)
    }

    test("Should return a list of files from raw") {
        val f = readAllFilesFromRaw(path.inputBasePath)
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
        val outcome = filesExists(path.inputBasePath, files)
        assertResult(Array("exampleTable"))(outcome)
    }

    test("Should return an empty array if no file coincidence") {
        val files = Array("E", "A")
        assertResult(Array[String]())(filesExists(path.inputBasePath, files))
    }
}
