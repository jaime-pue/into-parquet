/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller.ignore

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.common.SparkTestBuilder
import com.github.jaime.intoParquet.controller.HandleExecution
import com.github.jaime.intoParquet.model.Files
import com.github.jaime.intoParquet.model.enumeration.FallBackFail
import com.github.jaime.intoParquet.model.enumeration.ParseSchema
import com.github.jaime.intoParquet.model.enumeration.RawSchema

class TestIgnoreExecution extends SparkTestBuilder {

    private def testExecution(execution: HandleExecution): Unit = {
        try {
            execution.execution()
        } catch {
            case e: Exception => fail(e)
        }
    }

    test("Should finish even if errors found") {
        val files               = Resources.AllFiles
        val paths               = Resources.path
        val executionController = new IgnoreExecution(new Files(files), paths, new ParseSchema())
        testExecution(executionController)
    }

    test("Should finish if no table description and fallback is not Fail") {
        val files               = Array(Resources.onlyCSV)
        val paths               = Resources.path
        val executionController = new IgnoreExecution(new Files(files), paths, new ParseSchema())
        testExecution(executionController)
    }

    test("Should finish if no table description and fallback is Fail") {
        val files = Array(Resources.onlyCSV)
        val paths = Resources.path
        val executionController =
            new IgnoreExecution(new Files(files), paths, new ParseSchema(FallBackFail))
        testExecution(executionController)
    }

    test("Should finish if wrong table description but no fail fast") {
        val files               = Array(Resources.wrongTypeFile)
        val paths               = Resources.path
        val executionController = new IgnoreExecution(new Files(files), paths, new ParseSchema())
        testExecution(executionController)
    }

    test("Should work with raw schema") {
        val files               = Array(Resources.onlyCSV)
        val paths               = Resources.path
        val executionController = new IgnoreExecution(new Files(files), paths, RawSchema)
        testExecution(executionController)
    }

    test("Should finish if bad schema for table description") {
        val files = new Files(Array("badField"))
        val paths = Resources.path
        val exec  = new IgnoreExecution(files, paths, new ParseSchema())
        testExecution(exec)
    }

}
