package com.intoParquet.model.enumeration

sealed trait CastMode {

    override def toString: String = getClass.getSimpleName.replace("$", "")
}

object Raw extends CastMode

object InferSchema extends CastMode

object ReadSchema extends CastMode
