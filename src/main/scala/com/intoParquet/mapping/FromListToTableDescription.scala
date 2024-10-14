package com.intoParquet.mapping

import com.intoParquet.mapping.mapper.MapperTableDescription
import com.intoParquet.model.TableDescription

object FromListToTableDescription extends MapperTableDescription[List[String]]{

    override def castTo(value: List[String]): TableDescription = {
        val fileLines = deleteFirstLine(cleanLines(value))
        new TableDescription(fileLines)
    }
}
