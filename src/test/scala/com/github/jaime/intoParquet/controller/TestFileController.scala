/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.HandleFile.isBlank
import com.github.jaime.intoParquet.controller.HandleFile.splitFiles
import org.scalatest.funsuite.AnyFunSuite

class TestFileController extends AnyFunSuite {

    private val basePaths = Resources.path

    class TestFile(basePaths: BasePaths, csvFiles: Option[String], excludedFiles: Option[String] = None)
        extends HandleFile(basePaths, csvFiles, excludedFiles) {

        override def getRawFileNames: Seq[String] = List[String]()
    }

    test("Should get all csv files from resources folder") {
        val controller = new TestFile(basePaths,  None)
        val files = controller.getAllFilenamesFromFolder
        assume(files.isSuccess)
        assert(files.get.nonEmpty)
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
        val fileController = new TestFile(basePaths, Some("one"))
        val allFiles       = Array("one", "two")
        assertResult(Array("one"))(fileController.filterFiles(allFiles))
    }

    test("Should return all files") {
        val fileController = new TestFile(basePaths, None)
        val allFiles       = Array("one", "two")
        assertResult(allFiles)(fileController.filterFiles(allFiles))
    }

    test("Should exclude files by name") {
        val fileController = new TestFile(basePaths, None, Some("two"))
        val allFiles       = Array("one", "two")
        assertResult(Array("one"))(fileController.filterFiles(allFiles))
    }

    test("Should exclude a file that is included") {
        val fileController = new TestFile(basePaths, Some("one"), Some("one"))
        val allFiles       = Array("one", "two")
        assertResult(Array[String]())(fileController.filterFiles(allFiles))
    }
}
