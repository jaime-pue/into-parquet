/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.exception

import com.github.jaime.intoParquet.model.ParsedObject

sealed trait AppException extends Exception {
    self: Throwable =>
    val message: String

    override def getMessage: String = message
}

class NotImplementedTypeException(invalidType: String) extends AppException {
    override val message: String = s"Not recognized type conversion for $invalidType"
}

class WrongInputArgsException extends AppException {
    override val message: String = s"Input args parameters exception"
}

class NoFileFoundException(directory: String) extends AppException {

    override val message: String = s"No csv files found in: <$directory>"
}

class NoCSVException extends AppException {

    override val message: String = "At least one file is needed"
}

class NoSchemaFoundException(e: ParsedObject) extends AppException {

    override val message: String = s"No schema found for ${e.id}"
}
