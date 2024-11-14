/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.service

object Common {

    def sanitizeString(line: String): String = {
        line.trim.toLowerCase
    }
}
