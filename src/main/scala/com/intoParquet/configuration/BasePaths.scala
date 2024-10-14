package com.intoParquet.configuration

case class BasePaths(base: String = "./data"
) {
    val InputRawPath: String    = s"$base/input/raw/"
    val InputSchemaPath: String = s"$base/input/schema/"
    val OutputBasePath: String  = s"$base/output/"
}
