/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.service

import com.github.jaime.intoParquet.behaviour.AppLogger
import com.github.jaime.intoParquet.exception.NoFileFoundException

import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import scala.io.Source
import scala.jdk.CollectionConverters.IteratorHasAsScala

object FileLoader extends AppLogger {

    def readFile(filepath: String): Option[List[String]] = {
        try {
            val file = Source.fromFile(filepath)
            file.bufferedReader().lines()
            val lines = file.getLines().toList
            file.close()
            Some(lines)
        } catch {
            case _: Exception =>
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
