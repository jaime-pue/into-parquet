package com.intoParquet.mapping

import com.intoParquet.mapping.mapper.MapperTableDescription
import com.intoParquet.model.TableDescription

object FromStringToTableDescription extends MapperTableDescription[String]{

    override def castTo(value: String): TableDescription = {
        val fileLines = deleteFirstLine(cleanLines(value.split("\n").toList))
        new TableDescription(fileLines)
    }

    protected[mapping] def splitInLines(value: String): Array[String] = {
        value.split("\n")
    }
}
