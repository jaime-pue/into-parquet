package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.model.TableDescription
import com.github.jaime.intoParquet.model.TableDescription

import scala.util.matching.Regex

object IntoTableDescription {
    protected[mapping] def matchCase(line: String): Boolean = {
        val firstCol: Regex = raw"name\s+type(\s+comment)?.*".r
        line match {
            case firstCol(_*) => true
            case _            => false
        }
    }

    protected[mapping] def cleanLines(lines: List[String]): List[String] = {
        lines.map(l => l.trim()).filterNot(l => l.equals(""))
    }

    protected[mapping] def deleteFirstLine(lines: Seq[String]): Seq[String] = {
        lines.filterNot(l => matchCase(l))
    }

    def castTo(value: List[String]): TableDescription = {
        val fileLines = deleteFirstLine(cleanLines(value))
        new TableDescription(fileLines)
    }

    def castTo(value: String): TableDescription = {
        val fileLines = deleteFirstLine(cleanLines(value.split("\n").toList))
        new TableDescription(fileLines)
    }
}
