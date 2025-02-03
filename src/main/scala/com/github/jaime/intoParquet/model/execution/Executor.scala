/*
 * IntoParquet Copyright (c) 2024-2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.execution

import scala.util.Try

trait Executor {

    protected val file: String

    def cast: Try[Unit]

    override def toString: String = file
}
