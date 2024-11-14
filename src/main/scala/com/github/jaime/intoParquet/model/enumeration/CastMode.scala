/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.enumeration

sealed trait CastMode {

    override def toString: String = getClass.getSimpleName.replace("$", "")
}

object RawSchema extends CastMode

object InferSchema extends CastMode

class ParseSchema(val fallBack: FallBack = FallBackNone) extends CastMode {

    override def toString: String = s"${super.toString} with fallback: ${fallBack.toString}"

    override def equals(obj: Any): Boolean = {
        obj match {
            case a: ParseSchema => a.fallBack.equals(fallBack)
            case _ => false
        }
    }
}
