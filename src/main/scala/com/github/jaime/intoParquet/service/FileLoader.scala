/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.service

import com.github.jaime.intoParquet.service.AppLogger
import com.github.jaime.intoParquet.exception.NoFileFoundException

import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import scala.io.BufferedSource
import scala.io.Codec
import scala.io.Source
import scala.jdk.CollectionConverters.IteratorHasAsScala

object FileLoader extends AppLogger {

    /** Read text file from filepath. Without the implicit Codec
     * to UTF-8 scala method can't read some special chars,
     * like Á & Í*/
    def readFile(filepath: String): Option[List[String]] = {
        implicit val codec: Codec = Codec("UTF-8")
        try {
            val file: BufferedSource = Source.fromFile(filepath)
            logDebug(s"Read from $filepath")
            file.bufferedReader().lines()
            val lines = file.getLines().toList
            file.close()
            Some(lines)
        } catch {
            case i: Exception =>
                logDebug(i.getMessage)
                None
        }
    }

    def readAllFilesFromRaw(filepath: String): Array[String] = {
        val filePath = Paths.get(s"$filepath").toAbsolutePath
        logDebug(s"read from $filepath")
        try {
            Files
                .list(filePath)
                .iterator()
                .asScala
                .filter(isCSV)
                .map(grabFilename)
                .toArray
        } catch {
            case _: NoSuchFileException => throw new NoFileFoundException(filePath.toString)
        }
    }

    private def isCSV(f: Path): Boolean = {
        Files.isRegularFile(f) && f.getFileName.toString.endsWith(".csv")
    }

    def filesExists(filepath: String, files: Array[String]): Array[String] = {
        val allFiles = readAllFilesFromRaw(filepath)
        allFiles.filter(f => files.contains(f))
    }

    private def grabFilename(file: Path): String = {
        file.getFileName.toString.replace(".csv", "")
    }
}
