/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.common

import com.github.jaime.intoParquet.service.AppLogger
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.reflect.io.Directory

trait SparkTestBuilder extends AnyFunSuite with DataFrameSuiteBase {

    override implicit def reuseContextIfPossible: Boolean = true

    private def cleanDirectory(path: String): Boolean = {
        val directory = new Directory(new File(path))
        directory.deleteRecursively()
    }

    protected def buildDataFrame(rows: Seq[Row], schema: StructType): DataFrame = {
        val rdd = sc.parallelize(rows)
        spark.createDataFrame(rdd, schema)
    }

    protected def deactivateScLog(): Unit = sc.setLogLevel("ERROR")

    final override def afterAll(): Unit = {
        super.afterAll()
        cleanDirectory(Resources.path.outputBasePath)
    }

    override def beforeAll(): Unit = {
        super.beforeAll()
        deactivateScLog()
        AppLogger.DebugMode = true
    }
}
