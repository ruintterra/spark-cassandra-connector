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
  metadata: TableMetadata,
  optionalSchema: Option[StructType] = None) //Used for adding metadata references

  extends Table
    with SupportsRead
    with SupportsWrite {

  /*
  As of Spark 3.0

  While the correct thing to do here is probably to declare partition key columns as "non-nullable" I don't think spark is
  able to correctly handle this in a way that's easy to use. For example, if I make a dataframe using a "string" that column
  is now "Nullable" and there is very little you can do to change Spark's mind about this. For example

  scala> x.where('id.isNotNull).schema
  res10: org.apache.spark.sql.types.StructType = StructType(StructField(id,StringType,true), ...))

  Even though we have specifically removed all of the non-nulls the struct is locked to true
  Dropping nulls as well does nothing

  In the future lets fix this but only when Spark makes it possible to deal with it properly
  */
  override def schema(): StructType = optionalSchema
    .getOrElse(CassandraSourceUtil.toStructTypeAllNullable(metadata))

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
    TableCapability.ACCEPT_ANY_SCHEMA,
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.STREAMING_WRITE,
    TableCapability.TRUNCATE).asJava

  val tableDef = tableFromCassandra(connector, metadata.getKeyspace.asInternal(), name())

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    CassandraScanBuilder(session, catalogConf, tableDef, catalogName, options)
  }

  override def name(): String = metadata.getName.asInternal()

  override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder = {
    CassandraWriteBuilder(session, catalogConf, tableDef, catalogName, options)
  }
}

