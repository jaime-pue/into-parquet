/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model

class PairCSVAndTableDescription(_id: String, _schema: Option[TableDescription]) {

    val id: String                       = _id
    val schema: Option[TableDescription] = _schema
}
