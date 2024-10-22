package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.model.{ParsedObjectWrapper, TableDescription}
import com.github.jaime.intoParquet.model.{ParsedObject, ParsedObjectWrapper, TableDescription}
import com.github.jaime.intoParquet.service.FileLoader

object IntoParsedObjectWrapper {
    protected[mapping] def parseLine(
        elements: Array[String],
        fromPath: FileLoader
    ): ParsedObjectWrapper = {
        val i = elements.map(id => {
            val table: Option[TableDescription] = fromPath.readFile(id) match {
                case Some(value) => Some(IntoTableDescription.castTo(value))
                case None        => None
            }
            new ParsedObject(id, table)
        })
        new ParsedObjectWrapper(i)
    }

    def castTo(value: Array[String], fromPath: FileLoader): ParsedObjectWrapper = {
        parseLine(value, fromPath)
    }
}
