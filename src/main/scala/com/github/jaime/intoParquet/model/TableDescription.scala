/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model

import com.github.jaime.intoParquet.mapping.IntoTableDescription

class TableDescription(_fields: List[Field]) {

    val fields: List[Field] = _fields

    override def equals(obj: Any): Boolean = {
        obj match {
            case f: TableDescription => f.fields.equals(this.fields)
            case _                   => false
        }
    }

    def this(lines: Seq[String]) = {
        this(IntoTableDescription.mapFrom(lines.toList))
    }

    override def toString: String = {
        fields.mkString("\n")
    }
}
