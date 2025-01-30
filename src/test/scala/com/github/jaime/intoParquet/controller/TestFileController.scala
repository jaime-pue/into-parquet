/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.FileController.isBlank
import com.github.jaime.intoParquet.controller.FileController.splitFiles
import com.github.jaime.intoParquet.mapping.IntoBasePaths
import org.scalatest.funsuite.AnyFunSuite

class TestFileController extends AnyFunSuite {

    private val basePaths = Resources.path

    test("Should get all csv files from resources folder") {
        val controller = new FileController(basePaths,  None)
        val files = controller.getAllFilenamesFromFolder
        assume(files.isSuccess)
        assert(files.get.length > 0)
    }

    test("Should return None if directory doesn't exist") {
        val failController = new FileController(new BasePaths("imagine"), None)
        assert(failController.files.isEmpty)
    }

    test("Should return None if no file matches") {
        val controller = new FileController(basePaths, Some("one,two"))
        assert(controller.getFiles.isEmpty)
    }

    test("Should return all files if recursive set to false but no inclusion file set") {
        val controller = new FileController(basePaths, None, None)
        assert(controller.files.isDefined)
    }

    test("Should return all files if recursive set to false but only excluded files added") {
        val realFile = "exampleTable"
        val controller = new FileController(basePaths, None, Some(realFile))
        assume(controller.files.isDefined)
        assert(!controller.files.get.contains(realFile))
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

    test("Should return Array if input is a blank string") {
        assertResult(Array[String](""))(splitFiles(""))
    }

    test("Should return None if no files found") {
        val controller = new FileController(basePaths, Some("one,two"))
        assert(controller.files.isEmpty)
    }

    test("Should return None if point to empty directory, but log message") {
        val controller = new FileController(
          new BasePaths(new IntoBasePaths(Some(Resources.ResourceFolder))),
          
          csvFiles = None
        )
        assert(controller.files.isEmpty)
    }

    test("Should return true if is None") {
        assert(isBlank(None))
    }

    test("Should return true if empty") {
        assert(isBlank(Some("")))
    }

    test("Should return true if only whitespaces") {
        assert(isBlank(Some("   ")))
    }

    test("Should filter files by nane") {
        val fileController = new FileController(basePaths, Some("one"))
        val allFiles = Array("one", "two")
        assertResult(Array("one"))(fileController.filterFiles(allFiles))
    }

    test("Should return all files") {
        val fileController = new FileController(basePaths, None)
        val allFiles = Array("one", "two")
        assertResult(allFiles)(fileController.filterFiles(allFiles))
    }

    test("Should exclude files by name") {
        val fileController = new FileController(basePaths, None, Some("two"))
        val allFiles = Array("one", "two")
        assertResult(Array("one"))(fileController.filterFiles(allFiles))
    }

    test("Should exclude a file that is included") {
        val fileController = new FileController(basePaths, Some("one"), Some("one"))
        val allFiles = Array("one", "two")
        assertResult(Array[String]())(fileController.filterFiles(allFiles))
    }

    test("Should return None if input is only whitespaces") {
        val fileController = new FileController(basePaths, Some("   "), None)
        assert(fileController.files.isEmpty)
    }

    test("Should return a defined array if excluded files are only whitespaces") {
        val fileController = new FileController(basePaths, None, Some("  "))
        assert(fileController.files.isDefined)
    }
}
