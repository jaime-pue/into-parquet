package com.github.jaime.intoParquet.utils

import com.github.jaime.intoParquet.behaviour.AppLogger
import org.scalatest.funsuite.AnyFunSuite

class TestLogger extends AnyFunSuite with AppLogger {
    test("Should print to the console") {
        logInfo("HELLO")
    }

    test("Should not print debug to the console") {
        logDebug("ERROR")
    }
}
