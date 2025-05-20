/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller.fail

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.common.SparkTestBuilder
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.controller.HandleRouter
import com.github.jaime.intoParquet.exception.NoFileFoundException
import com.github.jaime.intoParquet.model.enumeration.RawSchema
import com.github.jaime.intoParquet.service.FileLoader

class TestFailRouter extends SparkTestBuilder {

    private def testController(c: HandleRouter): Unit = {
        try {
            c.route()
        } catch {
            case e: Exception =>
                e.getMessage
                fail(e)
        }
    }

    test("Should fail if input path is wrong") {
        val c = new FailFastRouter(new BasePaths("imagine"), RawSchema, None, None)
        assertThrows[NoFileFoundException](c.route())
    }

    test("Should not fail if include and exclude files match, so there are no files") {
        val files = Some("exampleTable")
        val c     = new FailFastRouter(Resources.path, RawSchema, files, files)
        testController(c)
    }

    test("Should finish if there is one included file") {
        val c = new FailFastRouter(Resources.path, RawSchema, Some("exampleTable"), None)
        assume(c.intoFileController.getFiles.get.items.head.equals("exampleTable"))
        testController(c)
    }

    test("Should finish but exclude one file") {
        val fileName = "exampleTable"
        val c        = new FailFastRouter(Resources.path, RawSchema, None, Some(fileName))
        assume(c.intoFileController.getFiles.isDefined)
        assume(FileLoader.readAllFilesFromRaw(Resources.InputTestFolder).contains(fileName))
        assert(!c.intoFileController.getFiles.get.items.contains(fileName))
    }
}
