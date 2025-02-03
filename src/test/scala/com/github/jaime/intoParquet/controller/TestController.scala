/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.common.SparkTestBuilder
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.exception.NoFileFoundException
import com.github.jaime.intoParquet.model.enumeration.RawSchema
import com.github.jaime.intoParquet.service.FileLoader

class TestController extends SparkTestBuilder {

    private def testController(c: Controller): Unit = {
        try {
            c.route()
        } catch {
            case e: Exception => fail(e)
        }
    }

    test("Should fail if input path is wrong") {
        val c = new Controller(new BasePaths("imagine"), true, RawSchema, None, None)
        assertThrows[NoFileFoundException](c.route())
    }

    test("Should finish if input path is wrong") {
        val c = new Controller(new BasePaths("imagine"), false, RawSchema, None, None)
        testController(c)
    }

    test("Should finish if include and exclude files match") {
        val files = Some("exampleTable")
        val c = new Controller(Resources.path, false, RawSchema, files, files)
        assert(c.intoFileController.getFiles.isEmpty)
    }

    test("Should fail if include and exclude files match") {
        val files = Some("exampleTable")
        val c = new Controller(Resources.path, true, RawSchema, files, files)
        assertThrows[NoFileFoundException](c.route())
    }

    test("Should finish if there is one included file") {
        val c = new Controller(Resources.path, true, RawSchema, Some("exampleTable"))
        assume(c.intoFileController.getFiles.get.items.head.equals("exampleTable"))
        testController(c)
    }

    test("Should finish but exclude one file") {
        val fileName = "exampleTable"
        val c = new Controller(Resources.path, true, RawSchema, None, Some(fileName))
        assume(c.intoFileController.getFiles.isDefined)
        assume(FileLoader.readAllFilesFromRaw(Resources.InputTestFolder).contains(fileName))
        assert(!c.intoFileController.getFiles.get.items.contains(fileName))
    }

}
