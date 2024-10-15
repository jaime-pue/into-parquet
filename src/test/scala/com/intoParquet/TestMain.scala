package com.intoParquet

import com.intoParquet.common.{Resources, SparkTestBuilder}
import com.intoParquet.exception.NoFileFoundException
import org.scalatest.BeforeAndAfterEach

class TestMain extends SparkTestBuilder with BeforeAndAfterEach {


    override protected def afterEach(): Unit = {
        Resources.cleanDirectory
    }

    private def buildTestArgs(files: Option[String] = None): Array[String] = {
        val input = Array("-p", Resources.ResourceFolder, "-f", files.getOrElse(""))
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

    test("Should fail if base path is wrong") {
        val args = Array("-p", "imagine")
        intercept[NoFileFoundException](Main.main(args))
    }
}
