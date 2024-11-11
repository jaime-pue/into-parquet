/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.model.ParsedObject
import com.github.jaime.intoParquet.model.TableDescription
import com.github.jaime.intoParquet.service.FileLoader

object IntoParsedObjectWrapper {

    def mapFrom(elements: Array[String], fromPath: FileLoader): Seq[ParsedObject] = {
        elements.map(id => {
            val table: Option[TableDescription] = fromPath.readFile(id) match {
                case Some(nameAndType) => Some(new TableDescription(nameAndType))
                case None              => None
            }
            new ParsedObject(id, table)
        })
    }
}
