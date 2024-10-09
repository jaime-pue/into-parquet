package com.intoParquet.model

import scala.util.matching.Regex

class TableDescription(_fields: Seq[String]) {

    val fields: Seq[String] = _fields

    def this(tableDescription: String) = {
        this(TableDescription.buildFromString(tableDescription))
    }
}

object TableDescription {

    protected[model] def deleteFirstLine(lines: Seq[String]): Seq[String] = {
        lines.filterNot(l => matchCase(l))
    }

    protected[model] def splitInLines(raw: String): Seq[String] = {
        raw.split("\n").map(l => l.trim()).filterNot(l => l.equals(""))
    }

    protected[model] def matchCase(line: String): Boolean = {
        val firstCol: Regex = raw"name\s+type\s+comment".r
        line match {
            case firstCol(_*) => true
            case _            => false
        }
    }

    protected[model] def buildFromString(raw: String): Array[String] = {
        val split     = splitInLines(raw)
        val cleanData = deleteFirstLine(split)
        cleanData.toArray
    }
}
