package com.datastax.spark.connector.datasource

import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.datasource.CassandraSourceUtil.consolidateConfs
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.writer.{TTLOption, TimestampOption, WriteConf}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra.CassandraSourceRelation.{TTLParam, WriteTimeParam}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, SupportsTruncate, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.util.Try
import scala.collection.JavaConverters._

case class CassandraWriteBuilder(
  session: SparkSession,
  catalogConf: SparkConf,
  tableDef: TableDef,
  catalogName: String,
  options: CaseInsensitiveStringMap,
  inputSchema: Option[StructType] = None)
extends WriteBuilder {

  val connectorConf = consolidateConfs(
    catalogConf,
    options.asCaseSensitiveMap().asScala.toMap,
    catalogName,
    tableDef.keyspaceName)

  val connector = CassandraConnector(connectorConf)
  val writeConf = WriteConf.fromSparkConf(connectorConf)

  override def withInputDataSchema(schema: StructType): WriteBuilder = {
    copy(inputSchema = Some(schema))
  }

  def getWrite() : CassandraBulkWrite = {
    val schema = inputSchema.getOrElse(throw new IllegalArgumentException("No Input Schema Given"))
    CassandraBulkWrite(session, connector, tableDef, writeConf, schema, catalogConf)
  }

  override def buildForBatch(): BatchWrite = getWrite()
  override def buildForStreaming(): StreamingWrite = getWrite()

}

case class CassandraBulkWrite(
  session: SparkSession,
  connector: CassandraConnector,
  tableDef: TableDef,
  writeConf: WriteConf,
  inputSchema: StructType,
  catalogConf: SparkConf)
  extends BatchWrite
    with StreamingWrite {


  private val ttlWriteOption =
    catalogConf.getOption(TTLParam.name)
      .map( value =>
        Try(value.toInt)
          .map(TTLOption.constant)
          .getOrElse(TTLOption.perRow(value)))
      .getOrElse(writeConf.ttl)

  private val timestampWriteOption =
    catalogConf.getOption(WriteTimeParam.name)
      .map( value =>
        Try(value.toLong)
          .map(TimestampOption.constant)
          .getOrElse(TimestampOption.perRow(value)))
      .getOrElse(writeConf.timestamp)

  val metadataEnrichedWriteConf = writeConf.copy(
    ttl = ttlWriteOption,
    timestamp = timestampWriteOption)

  def getWriterFactory(): CassandraDriverDataWriterFactory = {
    CassandraDriverDataWriterFactory(
      connector,
      tableDef,
      inputSchema,
      metadataEnrichedWriteConf
    )
  }

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = getWriterFactory()

  override def commit(messages: Array[WriterCommitMessage]): Unit = {} //No Commit in Cassandra Driver Writes

  override def abort(messages: Array[WriterCommitMessage]): Unit = {} //No Abort possible in Cassandra Driver Writes

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = getWriterFactory()

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
}
