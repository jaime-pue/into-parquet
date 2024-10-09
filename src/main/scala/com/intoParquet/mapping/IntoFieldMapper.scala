package com.intoParquet.mapping

import com.intoParquet.model.Field
import com.intoParquet.model.TableDescription
import com.intoParquet.model.FieldDescriptors

object IntoFieldMapper {

    def fromDescription(description: TableDescription): FieldDescriptors = {
        val fields = description.fields.map(f => {
            val e = splitValue(f)
            new Field(e(0), e(1))
        })
        new FieldDescriptors(fields)
    }

    protected[mapping] def splitValue(line: String): Array[String] = {
        line.split("\\s").take(2)
    }

}
