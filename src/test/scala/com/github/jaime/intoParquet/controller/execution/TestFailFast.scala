/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller.execution

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
        val process             = executionController.execution
        assert(process.isFailure)
        assertThrows[EnrichException](process.get)
    }

    test("Should finish if no table description and fallback is Fail") {
        val files = new Files(Array(Resources.onlyCSV))
        val paths = Resources.path
        val executionController =
            new FailFastExecution(files, paths, new ParseSchema(FallBackFail))
        val process = executionController.execution
        assert(process.isFailure)
        assertThrows[NoSchemaFoundException](process.get)
    }

}
