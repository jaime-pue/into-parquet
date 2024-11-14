/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.common.SparkTestBuilder
import com.github.jaime.intoParquet.exception.NoSchemaFoundException
import com.github.jaime.intoParquet.exception.NotImplementedTypeException
import com.github.jaime.intoParquet.model.enumeration.FallBackFail
import com.github.jaime.intoParquet.model.enumeration.ParseSchema
import com.github.jaime.intoParquet.model.enumeration.RawSchema

class TestExecutionController extends SparkTestBuilder {

    test("Should finish even if errors found") {
        val files               = Resources.AllFiles
        val paths               = Resources.path
        val executionController = new ExecutionController(files, paths, new ParseSchema(), false)
        assert(executionController.execution.isSuccess)
    }

    test("Should fail if error found while converting table description") {
        val files               = Array(Resources.wrongTypeFile)
        val paths               = Resources.path
        val executionController = new ExecutionController(files, paths, new ParseSchema(), true)
        val process             = executionController.execution
        assert(process.isFailure)
        assertThrows[NotImplementedTypeException](process.get)
    }

    test("Should finish if no table description and fallback is not Fail") {
        val files               = Array(Resources.onlyCSV)
        val paths               = Resources.path
        val executionController = new ExecutionController(files, paths, new ParseSchema(), true)
        val process             = executionController.execution
        assert(process.isSuccess)
    }

    test("Should finish if no table description and fallback is Fail") {
        val files = Array(Resources.onlyCSV)
        val paths = Resources.path
        val executionController =
            new ExecutionController(files, paths, new ParseSchema(FallBackFail), true)
        val process = executionController.execution
        assert(process.isFailure)
        assertThrows[NoSchemaFoundException](process.get)
    }

    test("Should finish if wrong table description but no fail fast") {
        val files               = Array(Resources.wrongTypeFile)
        val paths               = Resources.path
        val executionController = new ExecutionController(files, paths, new ParseSchema(), false)
        val process             = executionController.execution
        assert(process.isSuccess)
    }

    test("Should work with raw schema") {
        val files               = Array(Resources.onlyCSV)
        val paths               = Resources.path
        val executionController = new ExecutionController(files, paths, RawSchema, true)
        val process             = executionController.execution
        assert(process.isSuccess)
    }
}
