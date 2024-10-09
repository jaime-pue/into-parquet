package com.intoParquet.model

class FieldDescriptors(_fields: Seq[Field]) {

    val fields: Seq[Field] = _fields

    override def equals(obj: Any): Boolean = {
        obj match {
            case f: FieldDescriptors => f.fields.equals(this.fields)
            case _                   => false
        }
    }
}
