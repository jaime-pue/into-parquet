/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping.transformer

import com.github.jaime.intoParquet.service.Parser.InputArgs

trait AsController {
    def into: InputArgs
}
