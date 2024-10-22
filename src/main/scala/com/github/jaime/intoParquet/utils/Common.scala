package com.github.jaime.intoParquet.utils

object Common {

    def sanitizeString(line: String): String = {
        line.trim.toLowerCase
    }
}
