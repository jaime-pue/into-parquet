/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.service

import com.github.jaime.intoParquet.service.Common.renderPath
import com.github.jaime.intoParquet.service.Common.sanitizeString
import org.scalatest.funsuite.AnyFunSuite

class TestCommon extends AnyFunSuite {

    test("Should return an absolute path") {
        val relativePath = "./src/test"
        assert(!renderPath(relativePath).contains("."))
    }

    test("Should return the same string but lowercased") {
        assertResult("random")(sanitizeString("RanDom"))
    }

    test("Should work with digits in between") {
        assertResult("hell9")(sanitizeString("hell9"))
    }

    test("Should work with a lowercase version") {
        assertResult("world")(sanitizeString("world"))
    }

    test("Should clear left padding whitespaces") {
        assertResult("cat")(sanitizeString("   cat"))
    }

    test("Should work with right padding whitespaces") {
        assertResult("dog")(sanitizeString("dog   "))
    }
}
