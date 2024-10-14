package com.intoParquet.model

class ParsedObject(_id: String, _schema: Option[TableDescription]) {

    val id: String                       = _id
    val schema: Option[TableDescription] = _schema
}
