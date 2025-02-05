/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller.ignore

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.mapping.IntoBasePaths
import org.scalatest.funsuite.AnyFunSuite

class TestFileIgnore extends AnyFunSuite {

    private val basePaths = Resources.path


    test("Should return None if directory doesn't exist") {
        val failController = new IgnoreFile(new BasePaths("imagine"), None, None)
        assert(failController.getFiles.isEmpty)
    }

    test("Should return None if no file matches") {
        val controller = new IgnoreFile(basePaths, Some("one,two"), None)
        assert(controller.getFiles.isEmpty)
    }

    test("Should return all files") {
        val controller = new IgnoreFile(basePaths, None, None)
        assert(controller.getFiles.isDefined)
    }

    test("Should return all files with only excluded files") {
        val realFile = "exampleTable"
        val controller = new IgnoreFile(basePaths, None, Some(realFile))
        assume(controller.getFiles.isDefined)
        assert(!controller.getFiles.get.items.contains(realFile))
    }

    test("Should return None if no files found") {
        val controller = new IgnoreFile(basePaths, Some("one,two"), None)
        assert(controller.getFiles.isEmpty)
    }

    test("Should return None if point to empty directory, but log message") {
        val controller = new IgnoreFile(
            new BasePaths(new IntoBasePaths(Some(Resources.ResourceFolder))),
            csvFiles = None,
            excludedFiles = None
        )
        assert(controller.getFiles.isEmpty)
    }


    test("Should return None if input is only whitespaces") {
        val IgnoreFile = new IgnoreFile(basePaths, Some("   "), None)
        assert(IgnoreFile.getFiles.isEmpty)
    }

    test("Should return a defined array if excluded files are only whitespaces") {
        val IgnoreFile = new IgnoreFile(basePaths, None, Some("  "))
        assert(IgnoreFile.getFiles.isDefined)
    }
}
