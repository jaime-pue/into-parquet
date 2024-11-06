/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.exception.NoCSVException
import com.github.jaime.intoParquet.exception.NoFileFoundException
import com.github.jaime.intoParquet.utils.Parser.InputArgs
import org.scalatest.funsuite.AnyFunSuite

class TestIntoController extends AnyFunSuite {

    test("Should return an array of files") {
        val args           = InputArgs(Some("one,two"), recursive = false)
        val intoController = new IntoController(args)
        assertResult(Array("one", "two"))(intoController.getFilenamesFromInputLine)
    }

    test("Should maintain case") {
        val args           = InputArgs(Some("fileOne,fileTwo"), recursive = false)
        val intoController = new IntoController(args)
        assertResult(Array("fileOne", "fileTwo"))(intoController.getFilenamesFromInputLine)
    }

    test("Should remove duplicates") {
        val args           = InputArgs(Some("file,file"), recursive = false)
        val intoController = new IntoController(args)
        assertResult(Array("file"))(intoController.getFilenamesFromInputLine)
    }

    test("Should clear whitespaces") {
        val args           = InputArgs(Some("file,   file,file    "), recursive = false)
        val intoController = new IntoController(args)
        assertResult(Array("file"))(intoController.getFilenamesFromInputLine)
    }

    test("Should throw exception if no files set") {
        val args           = InputArgs(None, recursive = false)
        val intoController = new IntoController(args)
        assertThrows[NoCSVException](intoController.getFilenamesFromInputLine)
    }

    test("Should throw exception if no csv files in recursive mode") {
        val args           = InputArgs(None, inputDir = Some(Resources.ResourceFolder))
        val intoController = new IntoController(args)
        assertThrows[NoFileFoundException](intoController.getAllFilenamesFromFolder)
    }

}
