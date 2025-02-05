/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller.fail

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.common.SparkTestBuilder
import com.github.jaime.intoParquet.exception.EnrichException
import com.github.jaime.intoParquet.exception.NoSchemaFoundException
import com.github.jaime.intoParquet.model.Files
import com.github.jaime.intoParquet.model.enumeration.FallBackFail
import com.github.jaime.intoParquet.model.enumeration.ParseSchema

class TestFailFast extends SparkTestBuilder {

    test("Should fail if error found while converting table description") {
        val files               = new Files(Array(Resources.wrongTypeFile))
        val paths               = Resources.path
        val executionController = new FailFastExecution(files, paths, new ParseSchema())
        assertThrows[EnrichException](executionController.execution())
    }

    test("Should fail if no table description and fallback is Fail") {
        val files = new Files(Array(Resources.onlyCSV))
        val paths = Resources.path
        val executionController =
            new FailFastExecution(files, paths, new ParseSchema(FallBackFail))
        assertThrows[NoSchemaFoundException](executionController.execution())
    }

    test("Should fail if table description doesn't have a proper schema") {
        val files = new Files(Array("badField"))
        val paths = Resources.path
        val exec = new FailFastExecution(files, paths, new ParseSchema())
        assertThrows[EnrichException](exec.execution())
    }
}
