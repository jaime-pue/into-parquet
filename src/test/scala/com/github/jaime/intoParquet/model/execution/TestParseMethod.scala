/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import com.github.jaime.intoParquet.common.Resources
import com.github.jaime.intoParquet.common.SparkTestBuilder
import com.github.jaime.intoParquet.exception.NoSchemaFoundException
import com.github.jaime.intoParquet.exception.NotImplementedTypeException
import com.github.jaime.intoParquet.model.enumeration.FallBackFail
import com.github.jaime.intoParquet.model.enumeration.FallBackInfer
import com.github.jaime.intoParquet.model.enumeration.FallBackNone
import com.github.jaime.intoParquet.model.enumeration.FallBackRaw

class TestParseMethod extends SparkTestBuilder{

    private val basePaths = Resources.path

    test("Should do noting as fallback") {
        val parse = new Parse("timestampConversion", basePaths, FallBackNone)
        assert(parse.cast.isSuccess)
    }

    test("Should return a failure") {
        val parse = new Parse("timestampConversion", basePaths, FallBackFail)
        val e = parse.cast
        assert(e.isFailure)
        assertThrows[NoSchemaFoundException](e.get)
    }

    test("Should parse with infer") {
        val parse = new Parse("timestampConversion", basePaths, FallBackInfer)
        assert(parse.cast.isSuccess)
    }

    test("Should parse with raw") {
        val parse = new Parse("timestampConversion", basePaths, FallBackRaw)
        assert(parse.cast.isSuccess)
    }

    test("Should finish Ok if everything is right") {
        val parse = new Parse("exampleTable", basePaths, FallBackNone)
        assert(parse.cast.isSuccess)
    }

    test("Should be a failure if table description has wrong format but exists") {
        val parse = new Parse("wrongType", basePaths, FallBackNone)
        assert(parse.cast.isFailure)
        assertThrows[NotImplementedTypeException](parse.cast.get)
    }
}
