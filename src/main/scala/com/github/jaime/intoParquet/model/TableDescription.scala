/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model

import com.github.jaime.intoParquet.mapping.IntoTableDescription

import scala.util.Failure
import scala.util.Success
import scala.util.Try

class TableDescription(_fields: List[Field]) {

    val fields: List[Field] = _fields

    override def equals(obj: Any): Boolean = {
        obj match {
            case f: TableDescription => f.fields.equals(this.fields)
            case _                   => false
        }
    }

    protected[model] def this(lines: Seq[String]) = {
        this(IntoTableDescription.mapFrom(lines.toList))
    }

    override def toString: String = {
        fields.mkString("\n")
    }
}

object TableDescription {
    def fromLines(tableLines: List[String]): Try[TableDescription] = {
        val items  = IntoTableDescription.fromLines(tableLines).map {
            case Failure(exception) => return Failure(exception)
            case Success(value) => value
        }
        Success(new TableDescription(items))
    }
}
