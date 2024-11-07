/*
 * IntoParquet Copyright (c) 2024 Jaime Alvarez
 */

package com.github.jaime.intoParquet.common

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.reflect.io.Directory

trait SparkTestBuilder extends AnyFunSuite with DataFrameSuiteBase {

    private val SparkWarehouse: String = "./spark-warehouse"

    override implicit def reuseContextIfPossible: Boolean = true

    private def cleanDirectory(path: String): Boolean = {
        val directory = new Directory(new File(path))
        directory.deleteRecursively()
    }

    protected def buildDataFrame(rows: Seq[Row], schema: StructType): DataFrame = {

        val rdd = sc.parallelize(rows)
        spark.createDataFrame(rdd, schema)
    }

    protected def cleanCache(): Unit = {
        spark.sharedState.cacheManager.clearCache()
        spark.sessionState.catalog.reset()
    }

    protected def deactivateScLog(): Unit = sc.setLogLevel("ERROR")

    override def afterAll(): Unit = {
        super.afterAll()
        spark.sessionState.catalog.reset()
        cleanDirectory(SparkWarehouse)
        cleanDirectory(Resources.path.outputBasePath)
    }

    override def beforeAll(): Unit = {
        super.beforeAll()
        deactivateScLog()
    }
}
