/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.common.SparkTestBuilder
import com.github.jaime.intoParquet.exception.NoSchemaFoundException
import com.github.jaime.intoParquet.exception.WrongInputArgsException
import org.scalatest.BeforeAndAfterEach

class TestMain extends SparkTestBuilder with BeforeAndAfterEach {

    override protected def afterEach(): Unit = {
        Resources.cleanDirectory
    }

    private def buildTestArgs(files: Option[String] = None): Array[String] = {
        val input = Array(
          "-p",
          Resources.InputTestFolder,
          "-o",
          Resources.OutputTestFolder,
          "-f",
          files.getOrElse("")
        )
        input
    }

    private def buildTestArgs(files: String): Array[String] = {
        buildTestArgs(Some(files))
    }

    private def runMain(args: Array[String]): Unit = {
        try {
            Main.main(args)
        } catch {
            case ex: Exception => fail(ex)
        }
    }

    test("Should work in recursive mode") {
        val args = buildTestArgs()
        runMain(args)
    }

    test("Should work with a file") {
        val args = buildTestArgs("exampleTable")
        runMain(args)
    }

    test("Should finish and pass if no schema found") {
        val args = buildTestArgs("timestampConversion")
        runMain(args)
    }

    test("Should not fail if inputDir path is wrong") {
        val args = Array("-p", "imagine")
        runMain(args)
    }

    test("Should fail with fail-fast Mode and trying to apply to an unknown file") {
        val args = Array(
          "--fail-fast",
          "-f",
          "timestampConversion",
          "-fb",
          "fail",
          "-p",
          Resources.InputTestFolder
        )
        assertThrows[NoSchemaFoundException](Main.main(args))
    }

    test("Should not fail if can't file the file") {
        val args = Array("--fail-fast", "-f", "badRecord", "-m", "raw")
        runMain(args)
    }

    test("Should throw exception if args are wrong") {
        val args = Array("--random", "m")
        assertThrows[WrongInputArgsException](Main.main(args))
    }

    test("Should infer all files with recursive mode") {
        val args = Array(
          "--mode",
          "infer",
          "-p",
          Resources.InputTestFolder,
          "-o",
          Resources.OutputTestFolder,
          "--fail-fast"
        )
        runMain(args)
    }
}
