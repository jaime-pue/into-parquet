/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping.transformer

import com.github.jaime.intoParquet.controller.HandleRouter

trait Mapper[A <: HandleRouter] {

    def router(obj: AsController): A
}
