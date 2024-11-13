/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.exception

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

class NoSchemaFoundException(e: String) extends AppException {

    override val message: String = s"No schema found for ${e}"
}
