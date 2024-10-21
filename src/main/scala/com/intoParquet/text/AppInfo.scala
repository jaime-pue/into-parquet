package com.intoParquet.text

object AppInfo {

    private val version: String = "0.0.3"
    private val PacketDescription: String = {
        "Converts csv format files into parquet files, and can apply a schema when transforming them."
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
}
