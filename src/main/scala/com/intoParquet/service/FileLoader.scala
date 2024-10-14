package com.intoParquet.service

import com.intoParquet.configuration.BasePaths
import com.intoParquet.exception.NoFileFoundException

import java.nio.file.Paths
import scala.io.Source
import scala.util.{Failure, Success, Try}

object FileLoader {

    private val InputSchemaPath: String = BasePaths().InputSchemaPath

    def readFile(fileName: String): Option[List[String]] = {
        readFromDataFolder(fileName) match {
            case Failure(_)     => None
            case Success(value) => Some(value)
        }
    }

    private def readFromDataFolder(fileName: String): Try[List[String]] = {
        val filePath = resolveFilePath(fileName)

        try {
            val file = Source.fromFile(filePath)
            file.bufferedReader().lines()
            val lines = file.getLines().toList
            file.close()
            Success(lines)
        } catch {
            case _: Exception => Failure(new NoFileFoundException(filePath))
        }
    }

    private def resolveFilePath(fileName: String): String = {
        Paths.get(s"$InputSchemaPath$fileName").toAbsolutePath.toString
    }
}
