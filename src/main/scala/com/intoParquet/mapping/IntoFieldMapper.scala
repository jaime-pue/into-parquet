package com.intoParquet.mapping

import com.intoParquet.model.Field

object IntoFieldMapper {

    def fromDescription(description: String): Field = {
            val e = splitValue(description)
            new Field(e(0), e(1))
    }

    protected[mapping] def splitValue(line: String): Array[String] = {
        line.split("\\s").take(2)
    }

}
