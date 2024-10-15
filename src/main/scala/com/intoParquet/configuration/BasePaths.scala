package com.intoParquet.configuration

case class BasePaths(base: String = "./data"
) {
    private val InputRawPath: String    = s"$base/input/raw/"
    private val InputSchemaPath: String = s"$base/input/schema/"
    private val OutputBasePath: String  = s"$base/output/"

    def inputBasePath: String = {
        s"$base/input/"
    }

    def outputBasePath: String = {
        s"$base/output/"
    }
}
