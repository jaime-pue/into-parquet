/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.model.FieldWrapper
import com.github.jaime.intoParquet.model.{FieldWrapper, TableDescription}

object IntoFieldDescriptors {
    def fromDescription(description: TableDescription): FieldWrapper = {
        val values = description.fields.map(i => IntoFieldMapper.fromDescription(i))
        new FieldWrapper(values)
    }
}
