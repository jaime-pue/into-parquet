/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.behaviour

import scala.util.Try

trait Executor {

    protected val file: String

    final def cast: Try[Unit] = {
        Try(execution())
    }

    def execution(): Unit
}
