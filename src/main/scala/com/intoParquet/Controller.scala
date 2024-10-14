package com.intoParquet

import com.intoParquet.exception.NoFileFoundException
import com.intoParquet.mapping.IntoParsedObjectWrapper
import com.intoParquet.model.{
    InferSchema,
    ParsedObject,
    ParsedObjectWrapper,
    Raw,
    ReadSchema,
    WriteMode
}
import com.intoParquet.service.Converter.{
    executeInferSchema,
    executeRaw,
    executeWithTableDescription
}
import com.intoParquet.service.FileLoader
import com.intoParquet.utils.Parser.InputArgs

object Controller {

    def routeOnWriteMode(
        value: WriteMode,
        wrapper: ParsedObjectWrapper
    ): Unit = {
        wrapper.elements.foreach(e =>
            value match {
                case Raw         => executeRaw(e.id)
                case InferSchema => executeInferSchema(e.id)
                case ReadSchema  => readFromSchema(e)
            }
        )
    }

    private def readFromSchema(element: ParsedObject): Unit = {
        element.schema match {
            case Some(value) => executeWithTableDescription(element.id, value)
            case None        => executeRaw(element.id)
        }
    }

    def inputArgController(value: InputArgs): Unit = {
        val files = if (value.recursive) {
            IntoParsedObjectWrapper.castTo(FileLoader.readAllFilesFromRaw)
        } else {
            IntoParsedObjectWrapper.castTo(
              value.csvFile.getOrElse(throw new NoFileFoundException("v"))
            )
        }
        routeOnWriteMode(value.writeMethod, files)

    }
}
