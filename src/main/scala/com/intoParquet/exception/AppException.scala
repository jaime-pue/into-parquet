package com.intoParquet.exception

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

class NoFileFoundException(file: String) extends AppException {

    override val message: String = s"No csv files found in: <$file>"
}

class NoCSVException extends AppException {

    override val message: String = "At least one file is needed"
}