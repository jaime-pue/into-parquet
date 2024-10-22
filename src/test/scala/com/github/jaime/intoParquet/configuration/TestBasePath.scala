package com.github.jaime.intoParquet.configuration

import org.scalatest.funsuite.AnyFunSuite

class TestBasePath extends AnyFunSuite {

    private val defaultInput = "./data/input/"
    private val defaultOutput = "./data/output/"

    test("Should get new input folder") {
        val base = new BasePaths(Some("random"), None)
        assertResult("random")(base.inputBasePath)
    }

    test("Should default for none in both input and output") {
        val base = new BasePaths()
        assertResult(defaultInput)(base.inputBasePath)
        assertResult(defaultOutput)(base.outputBasePath)
    }

    test("Should change output if not none") {
        val base = new BasePaths(None, Some("some/output"))
        assertResult("some/output")(base.outputBasePath)
        assertResult(defaultInput)(base.inputBasePath)
    }

    test("Should change both output and input") {
        val base = new BasePaths(Some("random/input"), Some("other/output"))
        assertResult("random/input")(base.inputBasePath)
        assertResult("other/output")(base.outputBasePath)
    }

    test("Should change output to be inside of new input folder") {
        val base = new BasePaths(Some("random/input/"), None)
        assertResult("random/input/")(base.inputBasePath)
        assertResult("random/input/output/")(base.outputBasePath)
    }
}
