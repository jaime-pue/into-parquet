package com.intoParquet.text

import scala.io.Source

object AppInfo {

    private lazy val version: String = readVersion
    private val PacketDescription: String = {
        "Converts csv format files into parquet files, and can apply a schema when transforming them."
    }
    lazy val license: String =
        s"""into-parquet $version
           |
           |$PacketDescription
           |
           |Copyright (C) 2024 Free Software Foundation, Inc.
           |License GPLv3+: GNU GPL version 3 or later <https://gnu.org/licenses/gpl.html>.
           |This is free software: you are free to change and redistribute it.
           |There is NO WARRANTY, to the extent permitted by law.
           |
           |Written by Jaime Alvarez Fernandez.
           |""".stripMargin

    private def readVersion: String = {
        val file = Source.fromResource("info").mkString
        file.split("=").tail.head
    }
}
