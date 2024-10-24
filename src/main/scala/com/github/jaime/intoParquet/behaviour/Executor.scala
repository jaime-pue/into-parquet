package com.github.jaime.intoParquet.behaviour

import com.github.jaime.intoParquet.model.ParsedObject

import scala.util.Try

trait Executor {

    protected val element: ParsedObject

    def cast: Try[Unit]
}
