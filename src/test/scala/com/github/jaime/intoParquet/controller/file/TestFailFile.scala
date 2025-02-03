/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller.file

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.exception.NoFileFoundException
import org.scalatest.funsuite.AnyFunSuite

class TestFailFile extends AnyFunSuite {

    test("Should throw exception if points to wrong directory") {
        val c = new FailFastFile(new BasePaths("imagine"), None, None)
        assertThrows[NoFileFoundException](c.getFiles.get)
    }

    test("Should get all files if point to appropriate directory") {
        val c = new FailFastFile(Resources.path, None, None)
        assume(c.getFiles.isDefined)
        assert(c.getFiles.get.items.nonEmpty)
    }

    test("Should throw exception if no files found (path exists, include files is wrong)") {
        val wrongFile = "one"
        val c = new FailFastFile(Resources.path, Some(wrongFile), None)
        assume(!c.getAllFilenamesFromFolder.get.contains(wrongFile))
        assertThrows[NoFileFoundException](c.getFiles.get)
    }

    test("Should throw exception if exclude deletes included files") {
        val wrongFile = "exampleTable"
        val c = new FailFastFile(Resources.path, Some(wrongFile), Some(wrongFile))
        assume(c.getAllFilenamesFromFolder.get.contains(wrongFile))
        assertThrows[NoFileFoundException](c.getFiles.get)
    }
}
