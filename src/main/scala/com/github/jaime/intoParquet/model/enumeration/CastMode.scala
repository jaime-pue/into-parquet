/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.enumeration

sealed trait CastMode {

    override def toString: String = getClass.getSimpleName.replace("$", "")
}

object RawSchema extends CastMode

object InferSchema extends CastMode

class ParseSchema(val fallBack: Option[FallBack] = Some(FallBackNone)) extends CastMode {
    def this(fallBack: FallBack) = {
        this(Some(fallBack))
    }
}
