/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.model.enumeration.CastMode
import com.github.jaime.intoParquet.model.enumeration.FallBack
import com.github.jaime.intoParquet.model.enumeration.FallBackNone
import com.github.jaime.intoParquet.model.enumeration.InferSchema
import com.github.jaime.intoParquet.model.enumeration.ParseSchema
import com.github.jaime.intoParquet.model.enumeration.RawSchema

class IntoCastMode(fallBack: Option[FallBack], castMode: Option[CastMode]) {

    def mode: CastMode = {
        castMode match {
            case Some(mode) => resolveMode(mode)
            case None       => new ParseSchema(fallBack.getOrElse(FallBackNone))
        }
    }

    private def resolveMode(mode: CastMode): CastMode = {
        mode match {
            case InferSchema    => mode
            case RawSchema      => mode
            case _: ParseSchema => new ParseSchema(fallBack.getOrElse(FallBackNone))
        }
    }
}
