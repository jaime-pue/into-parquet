package com.intoParquet.mapping

import com.intoParquet.model.{FieldDescriptors, TableDescription}

object IntoFieldDescriptors {
    def fromDescription(description: TableDescription): FieldDescriptors = {
        val values = description.fields.map(i => IntoFieldMapper.fromDescription(i))
        new FieldDescriptors(values)
    }
}
