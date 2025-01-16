/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.configuration

/** Contains constant value configurations.
  *
  * Control specific configurations for spark reader methods. Even if there are vars, they should be
  * treated as IMMUTABLE val. Once they are set at the start of the flow, they should remains as it
  * is!
  */
object ReaderConfiguration {
    var Separator: String = ","

    val NullValue: String       = "NULL"
    val TimestampFormat: String = "yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]"
}
