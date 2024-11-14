/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.model.enumeration.FallBackFail
import com.github.jaime.intoParquet.model.enumeration.FallBackNone
import com.github.jaime.intoParquet.model.enumeration.FallBackRaw
import com.github.jaime.intoParquet.model.enumeration.InferSchema
import com.github.jaime.intoParquet.model.enumeration.ParseSchema
import com.github.jaime.intoParquet.model.enumeration.RawSchema
import org.scalatest.funsuite.AnyFunSuite

class TestIntoCastMode extends AnyFunSuite {

    test("Should return cast mode raw if there is no fallback") {
        assertResult(RawSchema)(new IntoCastMode(None, Some(RawSchema)).mode)
    }

    test("Should default to parse schema with fallback pass") {
        assertResult(new ParseSchema())(new IntoCastMode(None, Some(new ParseSchema())).mode)
    }

    test("Should return ParseSchema if fallback is defined but cast method is empty") {
        assertResult(new ParseSchema(FallBackFail))(new IntoCastMode(Some(FallBackFail), None).mode)
    }

    test("Should define behaviour cast mode not fallback if both are defined") {
        assertResult(InferSchema)(new IntoCastMode(Some(FallBackNone), Some(InferSchema)).mode)
    }

    test("Should return parse schema with fallback raw if both are defined") {
        assertResult(new ParseSchema(FallBackRaw))(
          new IntoCastMode(Some(FallBackRaw), Some(new ParseSchema())).mode
        )
    }

}
