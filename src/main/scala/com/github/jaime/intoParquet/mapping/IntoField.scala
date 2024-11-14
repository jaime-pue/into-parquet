/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.model.Field

object IntoField {

    def fromDescription(description: String): Field = {
        val e = splitValue(description)
        val dataType = IntoSQLDataType.mapFrom(e(1))
        new Field(e(0), dataType)
    }

    protected[mapping] def splitValue(line: String): Array[String] = {
        line.split("\\s").take(2)
    }
}
