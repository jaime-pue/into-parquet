/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.service

import java.nio.file.Paths

object Common {

    def sanitizeString(line: String): String = {
        line.trim.toLowerCase
    }

    /** toRealPath method implies the file exists or it will fail. Using the canonical path will
      * make it safe
      */
    def renderPath(path: String): String = {
        Paths.get(path).toAbsolutePath.normalize.toString
    }
}
