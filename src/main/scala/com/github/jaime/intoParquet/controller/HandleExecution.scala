/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.configuration.BasePaths
import com.github.jaime.intoParquet.model.Files
import com.github.jaime.intoParquet.model.enumeration.CastMode
import com.github.jaime.intoParquet.model.enumeration.InferSchema
import com.github.jaime.intoParquet.model.enumeration.ParseSchema
import com.github.jaime.intoParquet.model.enumeration.RawSchema
import com.github.jaime.intoParquet.model.execution.Executor
import com.github.jaime.intoParquet.model.execution.Infer
import com.github.jaime.intoParquet.model.execution.Parse
import com.github.jaime.intoParquet.model.execution.Raw
import com.github.jaime.intoParquet.service.AppLogger
import com.github.jaime.intoParquet.service.Chronometer

protected[controller] abstract class HandleExecution(
    csvFiles: Files,
    basePaths: BasePaths,
    castMode: CastMode
) extends AppLogger {

    /** If flows gets here, it does mean there are, at least, one file to
      * process. The script can time each item from read to write.
      */
    def execution(): Unit = {
        mapper.foreach(fileItem => {
            logInfo(s"Start job for: $fileItem")
            val timer = new Chronometer()
            processEachFile(fileItem)
            logInfo(s"$fileItem took: ${timer.toString} seconds")
        })
    }

    protected def processEachFile(e: Executor): Unit

    protected def mapper: Iterator[Executor] = {
        this.csvFiles.items.iterator.map(castElement)
    }

    private def castElement(element: String): Executor = {
        this.castMode match {
            case RawSchema      => new Raw(element, basePaths)
            case InferSchema    => new Infer(element, basePaths)
            case e: ParseSchema => new Parse(element, basePaths, e.fallBack)
        }
    }
}
