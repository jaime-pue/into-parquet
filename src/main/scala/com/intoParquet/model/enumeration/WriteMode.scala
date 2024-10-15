package com.intoParquet.model.enumeration

sealed trait WriteMode {

    override def toString: String = getClass.getSimpleName.replace("$", "")
}

object Raw extends WriteMode

object InferSchema extends WriteMode

object ReadSchema extends WriteMode
