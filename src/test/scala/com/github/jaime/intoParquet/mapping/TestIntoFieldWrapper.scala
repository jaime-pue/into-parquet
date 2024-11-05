/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.mapping

import com.github.jaime.intoParquet.model.{Field, FieldWrapper, TableDescription}
import org.scalatest.funsuite.AnyFunSuite

class TestIntoFieldWrapper extends AnyFunSuite {

    test("Should cast from a table description to several fields") {
        val describeTable =
            """
              |name type comment
              |random_name string
              |random_int int con comentario
              |""".stripMargin
        val description = IntoTableDescription.castTo(describeTable)
        val randomName  = new Field("random_name", "string")
        val randomInt   = new Field("random_int", "int")
        val expected    = new FieldWrapper(Seq(randomName, randomInt))
        assertResult(expected)(IntoFieldDescriptors.fromDescription(description))
    }

}
