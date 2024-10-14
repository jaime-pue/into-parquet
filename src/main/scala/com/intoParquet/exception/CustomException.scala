package com.intoParquet.exception

trait CustomException extends Exception {
    self: Throwable =>
    val message: String

    override def getMessage: String = message
}

class NotImplementedTypeException(invalidType: String) extends CustomException {
    override val message: String = s"Not recognized type conversion for $invalidType"
}

class WrongInputArgsException extends CustomException {
    override val message: String = s"Input args parameters exception"
}

class NoFileFoundException(file: String) extends CustomException {

    override val message: String = s"File not found: <$file>"
}