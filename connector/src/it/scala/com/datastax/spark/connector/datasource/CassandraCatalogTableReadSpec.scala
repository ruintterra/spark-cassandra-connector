package com.datastax.spark.connector.datasource

import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException}
import org.apache.spark.sql.execution.exchange.Exchange
import org.scalatest.concurrent.Eventually._

import scala.collection.JavaConverters._

class CassandraCatalogTableReadSpec extends CassandraCatalogSpecBase {

  val testTable = "testTable"

  def setupBasicTable(): Unit = {
    createDefaultKs()
    spark.sql(s"CREATE TABLE $defaultKs.$testTable (key Int, value STRING) PARTITIONED BY (key)")
    val ps = conn.withSessionDo(_.prepare(s"""INSERT INTO $defaultKs."$testTable" (key, value) VALUES (?, ?)"""))
    awaitAll {
      for (i <- 0 to 100) yield {
        executor.executeAsync(ps.bind(i : java.lang.Integer, i.toString))
      }
    }
  }

  "A Cassandra Catalog Table Read Support" should "initialize successfully" in {
    spark.sessionState.catalogManager.currentCatalog.name() should include("Catalog cassandra")
  }

  it should "read from an empty table" in {
    createDefaultKs()
    spark.sql(s"CREATE TABLE $defaultKs.$testTable (key Int, value STRING) PARTITIONED BY (key)")
    spark.sql(s"SELECT * FROM $defaultKs.$testTable").collect() shouldBe empty
  }

  it should "read from a table with some data" in {
    setupBasicTable()
    val results = spark.sql(s"SELECT * FROM $defaultKs.$testTable").collect()
    val expected = for (i <- 0 to 100) yield (i, i.toString)
    results.map( row => (row.getInt(0), row.getString(1))) should contain theSameElementsAs(expected)
  }

  it should "correctly use partitioning info" in {
    setupBasicTable()

    //Because partitioning supports this aggregate should not require a shuffle
    spark.sql(s"SELECT DISTINCT key FROM $defaultKs.$testTable")
      .queryExecution
      .executedPlan
      .collectFirst{ case exchange: Exchange => exchange } shouldBe empty

    //Because partitioning does not support this aggregate there should be a shuffle
    spark.sql(s"SELECT DISTINCT value FROM $defaultKs.$testTable").explain
    spark.sql(s"SELECT DISTINCT value FROM $defaultKs.$testTable")
      .queryExecution
      .executedPlan
      .collectFirst{ case exchange: Exchange => exchange } shouldBe defined
  }

}
