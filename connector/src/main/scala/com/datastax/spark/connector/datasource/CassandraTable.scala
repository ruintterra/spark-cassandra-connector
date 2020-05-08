package com.datastax.spark.connector.datasource

import java.util

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.util._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

case class CassandraTable(
  session: SparkSession,
  catalogConf: SparkConf,
  connector: CassandraConnector,
  catalogName: String,
  metadata: TableMetadata)

  extends Table
    with SupportsRead
    with SupportsWrite {

  override def schema(): StructType = CassandraSourceUtil.toStructType(metadata)

  override def partitioning(): Array[Transform] = {
    metadata.getPartitionKey.asScala
      .map(_.getName.asInternal())
      .map(Expressions.identity)
      .toArray
  }

  override def properties(): util.Map[String, String] = {
    val clusteringKey = metadata.getClusteringColumns().asScala
      .map { case (col, ord) => s"${col.getName.asInternal}.$ord" }
      .mkString("[", ",", "]")
    (
      Map("clustering_key" -> clusteringKey)
        ++ metadata.getOptions.asScala.map { case (key, value) => key.asInternal() -> value.toString }
      ).asJava
  }

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.STREAMING_WRITE,
    TableCapability.TRUNCATE).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val tableDef = tableFromCassandra(connector, metadata.getKeyspace.asInternal(), name())
    CassandraScanBuilder(session, catalogConf, tableDef, catalogName, options)
  }

  override def name(): String = metadata.getName.asInternal()

  override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder = {
    val tableDef = tableFromCassandra(connector, metadata.getKeyspace.asInternal(), name())
    CassandraWriteBuilder(session, catalogConf, tableDef, catalogName, options)
  }
}

