package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.model.Field
import com.github.jaime.intoParquet.model.Field

object IntoFieldMapper {

    def fromDescription(description: String): Field = {
            val e = splitValue(description)
            new Field(e(0), e(1))
    }

    protected[mapping] def splitValue(line: String): Array[String] = {
        line.split("\\s").take(2)
    }

}
