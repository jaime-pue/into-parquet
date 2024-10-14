package com.intoParquet.model.enumeration

sealed trait WriteMode {

    override def toString: String = getClass.toString
}

object Raw extends WriteMode

object InferSchema extends WriteMode

object ReadSchema extends WriteMode
