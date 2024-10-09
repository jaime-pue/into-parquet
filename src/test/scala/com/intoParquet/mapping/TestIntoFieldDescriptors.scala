package com.intoParquet.mapping

import com.intoParquet.model.{Field, FieldDescriptors, TableDescription}
import org.scalatest.funsuite.AnyFunSuite

class TestIntoFieldDescriptors extends AnyFunSuite {

    test("Should cast from a table description to several fields") {
        val describeTable =
            """
              |name type comment
              |random_name string
              |random_int int con comentario
              |""".stripMargin
        val description = new TableDescription(describeTable)
        val randomName  = new Field("random_name", "string")
        val randomInt   = new Field("random_int", "int")
        val expected    = new FieldDescriptors(Seq(randomName, randomInt))
        assertResult(expected)(IntoFieldDescriptors.fromDescription(description))
    }

}
