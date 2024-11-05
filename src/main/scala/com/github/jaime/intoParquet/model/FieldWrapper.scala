/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model

/**
 * Wrapper for [[com.intoParquet.model.Field]]
*/
class FieldWrapper(_fields: Seq[Field]) {

    val fields: Seq[Field] = _fields

    override def equals(obj: Any): Boolean = {
        obj match {
            case f: FieldWrapper => f.fields.equals(this.fields)
            case _                   => false
        }
    }
}
