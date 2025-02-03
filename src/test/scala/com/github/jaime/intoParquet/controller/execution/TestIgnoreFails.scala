/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller.execution

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.common.SparkTestBuilder
import com.github.jaime.intoParquet.model.Files
import com.github.jaime.intoParquet.model.enumeration.FallBackFail
import com.github.jaime.intoParquet.model.enumeration.ParseSchema
import com.github.jaime.intoParquet.model.enumeration.RawSchema

class TestIgnoreFails extends SparkTestBuilder {


    test("Should finish even if errors found") {
        val files               = Resources.AllFiles
        val paths               = Resources.path
        val executionController = new IgnoreExecution(new Files(files), paths, new ParseSchema())
        assert(executionController.execution.isSuccess)
    }


    test("Should finish if no table description and fallback is not Fail") {
        val files               = Array(Resources.onlyCSV)
        val paths               = Resources.path
        val executionController = new IgnoreExecution(new Files(files), paths, new ParseSchema())
        val process             = executionController.execution
        assert(process.isSuccess)
    }

    test("Should finish if no table description and fallback is Fail") {
        val files = Array(Resources.onlyCSV)
        val paths = Resources.path
        val executionController =
            new IgnoreExecution(new Files(files), paths, new ParseSchema(FallBackFail))
        val process = executionController.execution
        assert(process.isSuccess)
    }

    test("Should finish if wrong table description but no fail fast") {
        val files               = Array(Resources.wrongTypeFile)
        val paths               = Resources.path
        val executionController = new IgnoreExecution(new Files(files), paths, new ParseSchema())
        val process             = executionController.execution
        assert(process.isSuccess)
    }

    test("Should work with raw schema") {
        val files               = Array(Resources.onlyCSV)
        val paths               = Resources.path
        val executionController = new IgnoreExecution(new Files(files), paths, RawSchema)
        val process             = executionController.execution
        assert(process.isSuccess)
    }

}
