package com.intoParquet.model.enumeration

sealed trait CastMode {

    override def toString: String = getClass.getSimpleName.replace("$", "")
}

object Raw extends CastMode

object InferSchema extends CastMode

class ParseSchema(val fallBack: Option[FallBack] = Some(FallBackRaw)) extends CastMode {
    def this(fallBack: FallBack) = {
        this(Some(fallBack))
    }
}
