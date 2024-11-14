/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping.transformer

trait AsBasePath {

    def inputBasePath: String

    def outputBasePath: String
}
