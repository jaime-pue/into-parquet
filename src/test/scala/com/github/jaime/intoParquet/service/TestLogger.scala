/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.service

import com.github.jaime.intoParquet.service.AppLogger
import org.scalatest.funsuite.AnyFunSuite

class TestLogger extends AnyFunSuite with AppLogger {
    test("Should print to the console") {
        logInfo("HELLO")
    }

    test("Should not print debug to the console") {
        logDebug("ERROR")
    }
}
