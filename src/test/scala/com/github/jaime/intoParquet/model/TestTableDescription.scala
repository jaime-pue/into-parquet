/*
 * IntoParquet Copyright (c) 2025 Jaime Alvarez
 */

package com.github.jaime.intoParquet.model

import com.github.jaime.intoParquet.model.enumeration.DecimalDataType
import com.github.jaime.intoParquet.model.enumeration.IntegerDataType
import com.github.jaime.intoParquet.model.enumeration.StringDataType
import org.scalatest.funsuite.AnyFunSuite

class TestTableDescription extends AnyFunSuite {

    test("Should cast from a table description to several fields") {
        val describeTable =
            """
              |name type comment
              |random_name string
              |random_int int con comentario
              |""".stripMargin
        val description = new TableDescription(describeTable.split("\n"))
        val randomName  = new Field("random_name", StringDataType)
        val randomInt   = new Field("random_int", IntegerDataType)
        val expected    = new TableDescription(List(randomName, randomInt))
        assertResult(expected)(description)
    }

    test("Should transform decimal with spaces") {
        val describeTable = List("random_dec DECIMAL(35, 2) COMMENT 'buh'")
        val description = new TableDescription(describeTable)
        val randomDec   = new Field("random_dec", new DecimalDataType(35, 2))
        val expected    = new TableDescription(List(randomDec))
        assertResult(expected)(description)

    }
}
