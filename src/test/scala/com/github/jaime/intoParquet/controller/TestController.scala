package com.github.jaime.intoParquet.controller

import com.github.jaime.intoParquet.common.{Resources, SparkTestBuilder}
import com.github.jaime.intoParquet.model.{ParsedObject, ParsedObjectWrapper}
import com.github.jaime.intoParquet.model.enumeration.Raw

class TestController extends SparkTestBuilder {

    test("Should iterate over all files even if error") {
        val goodFile = new ParsedObject("exampleTable", None)
        val badFile  = new ParsedObject("badFile", None)
        val wrapper  = new ParsedObjectWrapper(Seq(goodFile, badFile, goodFile))
        val controller: Controller = new Controller(Resources.path, Raw, wrapper, false)
        assert(controller.execution.isSuccess)
    }

    test("Should fail with fail fast mode") {
        val goodFile = new ParsedObject("exampleTable", None)
        val badFile  = new ParsedObject("badFile", None)
        val wrapper  = new ParsedObjectWrapper(Seq(goodFile, badFile, goodFile))
        val controller: Controller = new Controller(Resources.path, Raw, wrapper, true)
        assert(controller.execution.isFailure)
    }
}
