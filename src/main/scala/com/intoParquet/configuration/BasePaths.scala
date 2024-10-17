package com.intoParquet.configuration

class BasePaths(
    inputDir: Option[String] = None,
    outputDir: Option[String] = None
) {
    private val InputRawPath: String    = s"$inputDir/input/raw/"
    private val InputSchemaPath: String = s"$inputDir/input/schema/"
    private val OutputBasePath: String  = s"$inputDir/output/"

    private val DefaultInput: String  = "./data/input/"
    private val DefaultOutput: String = "./data/output/"

    def this(base: String) = {
        this(Some(s"${base}input/"), Some(s"${base}output/"))
    }

    def inputBasePath: String = {
        inputDir.getOrElse(DefaultInput)
    }

    def outputBasePath: String = {
        if (inputDir.isDefined && outputDir.isEmpty) {
            s"${inputBasePath}output/"
        } else {
            outputDir.getOrElse(DefaultOutput)
        }
    }
}
