/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet

import com.github.jaime.intoParquet.common.{Resources, SparkTestBuilder}
import com.github.jaime.intoParquet.exception.NoFileFoundException
import org.scalatest.BeforeAndAfterEach

class TestMain extends SparkTestBuilder with BeforeAndAfterEach {


    override protected def afterEach(): Unit = {
        Resources.cleanDirectory
    }

    private def buildTestArgs(files: Option[String] = None): Array[String] = {
        val input = Array("-p", Resources.InputTestFolder, "-o", Resources.OutputTestFolder, "-f", files.getOrElse(""))
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

    test("Should fail if inputDir path is wrong") {
        val args = Array("-p", "imagine")
        intercept[NoFileFoundException](Main.main(args))
    }

    test("Should fail with fail-fast Mode") {
        val args = Array("--fail-fast", "-f", "badRecord", "-fb", "fail")
        intercept[Exception](Main.main(args))
    }

    test("Should fail with fail-fast") {
        val args = Array("--fail-fast", "-f", "badRecord", "-m", "raw")
        intercept[Exception](Main.main(args))
    }
}
