/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.text

import scala.io.Source

object AppInfo {

    private val version: String = readVersion
    private val PacketDescription: String = {
        "Converts csv format files into parquet files, and can apply a schema when transforming them."
    }

    private def readVersion: String = {
        val file = Source.fromResource("info").mkString
        file.split("=").tail.head
    }

    val license: String =
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

    val Example: String =
        s"""
          |[How to]
          |Create a directory structure as follows:
          |  ./data/input/
          |Script will automatically create output folder inside ./data/
          |Add csv files inside ./data/input/ together with a text file with the same name that contains the schema.
          |Execute the app within the shell
          |java -jar target/into-parquet-$version-jar-with-dependencies.jar
          |Parquet files will appear inside ./data/output/ directory with the same name as the csv file
          |
          |Default options:
          | > Read all csv files found
          | > Input path ./data/input/
          | > Output path ./data/output/
          | > Cast method parse schema
          | > Fallback method None
          | > Fail-fast set to false
          |""".stripMargin
}
