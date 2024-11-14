/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model.enumeration

sealed trait FallBack {
    override def toString: String = {
        getClass.getSimpleName.toLowerCase.replace("$", "").replace("fallback", "").capitalize
    }

    override def equals(obj: Any): Boolean = super.equals(obj)
}

object FallBackRaw extends FallBack

object FallBackInfer extends FallBack

object FallBackFail extends FallBack

object FallBackNone extends FallBack
