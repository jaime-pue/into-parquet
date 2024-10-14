package com.intoParquet.model

sealed trait WriteMode

object Raw extends WriteMode

object InferSchema extends WriteMode

object ReadSchema extends WriteMode
