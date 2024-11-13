/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.FileController.splitFiles
import org.scalatest.funsuite.AnyFunSuite

class TestFileController extends AnyFunSuite {

    private val basePaths = Resources.path

    test("Should get all csv files from resources folder") {
        val controller = new FileController(basePaths, recursiveRead = true, None)
        assert(controller.getAllFilenamesFromFolder.length > 0)
    }

    test("Should return an empty array of files") {
        val controller = new FileController(basePaths, recursiveRead = true, Some("one,two"))
        assertResult(Array[String]())(controller.getFilenamesFromInputLine)
    }

    test("Should return an array of files") {
        val files = "one,two"
        assertResult(Array("one", "two"))(splitFiles(files))
    }

    test("Should maintain case") {
        val files = "fileOne,fileTwo"
        assertResult(Array("fileOne", "fileTwo"))(splitFiles(files))
    }

    test("Should remove duplicates") {
        val files = "file,file"
        assertResult(Array("file"))(splitFiles(files))
    }

    test("Should clear whitespaces") {
        val files = "file,   file,file    "
        assertResult(Array("file"))(splitFiles(files))
    }

    test("Should return empty Array if input is an empty Array") {
        val files = ""
        assertResult(Array[String]())(splitFiles(files))
    }

    test("Should return None if no files found") {
        val controller = new FileController(basePaths, recursiveRead = false, Some("one,two"))
        assert(controller.files.isEmpty)
    }

    test("Should return None if point to empty directory, but log message") {
        val controller = new FileController(
          new BasePaths(Some(Resources.ResourceFolder)),
          recursiveRead = true,
          csvFiles = None
        )
        assert(controller.files.isEmpty)
    }

    test("Should throw exception if no recursive read and no files") {
        val controller = new FileController(basePaths, recursiveRead = false, None)
        assertResult(Array[String]())(controller.getFilenamesFromInputLine)
        assert(controller.files.isEmpty)
    }

}
