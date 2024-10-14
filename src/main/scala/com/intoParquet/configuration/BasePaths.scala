package com.intoParquet.configuration

case class BasePaths(
    InputRawPath: String = "./data/input/raw/",
    InputSchemaPath: String = "./data/input/schema/",
    OutputBasePath: String = "./data/output/"
) {}
