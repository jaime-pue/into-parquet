package com.intoParquet.mapping

import com.intoParquet.model.{FieldWrapper, TableDescription}

object IntoFieldDescriptors {
    def fromDescription(description: TableDescription): FieldWrapper = {
        val values = description.fields.map(i => IntoFieldMapper.fromDescription(i))
        new FieldWrapper(values)
    }
}
