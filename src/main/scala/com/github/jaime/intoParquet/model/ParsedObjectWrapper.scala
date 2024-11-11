/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model

import com.github.jaime.intoParquet.mapping.IntoParsedObjectWrapper
import com.github.jaime.intoParquet.service.FileLoader

class ParsedObjectWrapper(_elements: Seq[ParsedObject]) {
    val elements: Seq[ParsedObject] = _elements

    def this(filenames: Array[String], fromPath: FileLoader) = {
        this(IntoParsedObjectWrapper.mapFrom(filenames, fromPath))
    }
}
