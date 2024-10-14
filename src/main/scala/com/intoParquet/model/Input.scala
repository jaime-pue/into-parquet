package com.intoParquet.model

class Input(files: List[String]) {
    def this(inputArg: String) = {
        this(inputArg.split(";").map(_.trim).toList)
    }
}
