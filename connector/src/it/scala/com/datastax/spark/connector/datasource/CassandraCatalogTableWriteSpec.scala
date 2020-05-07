package com.datastax.spark.connector.datasource

import org.apache.spark.sql.execution.exchange.Exchange

class CassandraCatalogTableWriteSpec extends CassandraCatalogSpecBase {

  val testTable = "testTable"

  def setupBasicTable(): Unit = {
    createDefaultKs(1)
    spark.sql(s"CREATE TABLE $defaultKs.$testTable (key Int, value STRING) PARTITIONED BY (key)")
    val ps = conn.withSessionDo(_.prepare(s"""INSERT INTO $defaultKs."$testTable" (key, value) VALUES (?, ?)"""))
    awaitAll {
      for (i <- 0 to 100) yield {
        executor.executeAsync(ps.bind(i : java.lang.Integer, i.toString))
      }
    }
  }

  "A Cassandra Catalog Table Write Support" should "initialize successfully" in {
    spark.sessionState.catalogManager.currentCatalog.name() should include("Catalog cassandra")
  }

  it should "support CTAS" in {
    val outputTable = "output_empty"
    setupBasicTable()
    spark.sql(
      s"""CREATE TABLE $defaultKs.$outputTable PARTITIONED BY (key)
         |AS SELECT * FROM $defaultKs.$testTable""".stripMargin)
    val results = spark.sql(s"""SELECT * FROM $defaultKs.$outputTable""").collect()
    results.length shouldBe (101)
  }

}
