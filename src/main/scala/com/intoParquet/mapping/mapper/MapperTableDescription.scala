package com.intoParquet.mapping.mapper

import com.intoParquet.model.TableDescription

import scala.util.matching.Regex

trait MapperTableDescription[A] {
    def castTo(value: A): TableDescription

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
}
