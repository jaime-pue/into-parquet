package com.intoParquet.mapping

import com.intoParquet.model.{ParsedObject, ParsedObjectWrapper, TableDescription}
import com.intoParquet.service.FileLoader

object IntoParsedObjectWrapper {
    protected[mapping] def parseLine(elements: Array[String]): ParsedObjectWrapper = {
        val i = elements.map(id => {
            val table: Option[TableDescription] = FileLoader.readFile(id) match {
                case Some(value) => Some(IntoTableDescription.castTo(value))
                case None        => None
            }
            new ParsedObject(id, table)
        })
        new ParsedObjectWrapper(i)
    }

    def castTo(value: String): ParsedObjectWrapper = {
        val elements = value.split(";").map(_.trim)
        parseLine(elements)
    }

    def castTo(value: Array[String]): ParsedObjectWrapper = {
        parseLine(value)
    }
}
