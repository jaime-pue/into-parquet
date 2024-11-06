/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.service

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.exception.NoFileFoundException
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

    test("Should fail if inputDir path doesn't exist") {
        val wrongDirectory: FileLoader = new FileLoader(new BasePaths("Imagine"))
        val f = wrongDirectory.readAllFilesFromRaw
        assume(f.isFailure)
        assertThrows[NoFileFoundException](f.get)
    }

    test("Should fail if no csv files found") {
        val wrongDirectory: FileLoader = new FileLoader(new BasePaths(Some(Resources.ResourceFolder)))
        val f = wrongDirectory.readAllFilesFromRaw
        assume(f.isFailure)
        assertThrows[NoFileFoundException](f.get)
    }
}
