/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.model.Field

import scala.util.matching.Regex

object IntoTableDescription {
    protected[mapping] def matchCase(line: String): Boolean = {
        val firstCol: Regex = raw"name\s+type(\s+comment)?.*".r
        line match {
            case firstCol(_*) => true
            case _            => false
        }
    }

    private def cleanLines(lines: List[String]): List[String] = {
        lines.map(l => l.trim()).filterNot(l => l.equals(""))
    }

    protected[mapping] def deleteFirstLine(lines: Seq[String]): Seq[String] = {
        lines.filterNot(l => matchCase(l))
    }

    def mapFrom(lines: List[String]): List[Field] = {
        val fileLines = deleteFirstLine(cleanLines(lines)).toList
        intoFields(fileLines)
    }

    private def intoFields(lines: List[String]): List[Field] = {
        lines.map(i => IntoField.fromDescription(i))
    }
}
