/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.behaviour

import com.github.jaime.intoParquet.model.PairCSVAndTableDescription

import scala.util.Try

trait Executor {

    protected val element: PairCSVAndTableDescription

    final def cast: Try[Unit] = {
        Try(execution())
    }

    protected def execution(): Unit
}
