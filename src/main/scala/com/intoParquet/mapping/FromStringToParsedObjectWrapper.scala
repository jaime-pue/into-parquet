package com.intoParquet.mapping

import com.intoParquet.model.{ParsedObject, ParsedObjectWrapper, TableDescription}
import com.intoParquet.service.FileLoader

object FromStringToParsedObjectWrapper {

    def castTo(value: String): ParsedObjectWrapper = {
        val elements = value.split(";").map(_.trim)
        val i = elements.map(id => {
            val table: Option[TableDescription] = FileLoader.readFile(id) match {
                case Some(value) => Some(FromListToTableDescription.castTo(value))
                case None        => None
            }
            new ParsedObject(id, table)
        })
        new ParsedObjectWrapper(i)
    }
}
