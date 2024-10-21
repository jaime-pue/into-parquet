package com.intoParquet.utils

object Common {

    def sanitizeString(line: String): String = {
        line.trim.toLowerCase
    }
}
