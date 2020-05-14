package com.datastax.spark.connector.datasource

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.datasource.CassandraSourceUtil.consolidateConfs
import com.datastax.spark.connector.writer.{TTLOption, TimestampOption, WriteConf}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra.CassandraSourceRelation
import org.apache.spark.sql.cassandra.CassandraSourceRelation.{TTLParam, WriteTimeParam}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.util.Try

case class CassandraWriteBuilder(
  session: SparkSession,
  catalogConf: SparkConf,
  tableDef: TableDef,
  catalogName: String,
  options: CaseInsensitiveStringMap,
  inputSchema: Option[StructType] = None)
  extends WriteBuilder with SupportsTruncate {

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

  override def buildForBatch(): BatchWrite = getWrite()

  def getWrite(): CassandraBulkWrite = {
    val schema = inputSchema.getOrElse(throw new IllegalArgumentException("No Input Schema Given"))
    CassandraBulkWrite(session, connector, tableDef, writeConf, schema, catalogConf)
  }

  override def buildForStreaming(): StreamingWrite = getWrite()

  /**
    * With Cassandra we cannot actually do this before commit since we are writing constantly,
    * our best option is to truncate now. Since we have no notion of rollbacks this is probably
    * the best we can do.
    **/
  override def truncate(): WriteBuilder = {
    if (connectorConf.getOption("confirm.truncate").getOrElse("false").toBoolean) {
      connector.withSessionDo(session =>
        session.execute(QueryBuilder.truncate(CqlIdentifier.fromInternal(tableDef.keyspaceName), CqlIdentifier.fromInternal(tableDef.tableName)).asCql()))
      this
    }  else {
      throw new UnsupportedOperationException(
        """You are attempting to use overwrite mode which will truncate
          |this table prior to inserting data. If you would merely like
          |to change data already in the table use the "Append" mode.
          |To actually truncate please pass in true value to the option
          |"confirm.truncate" when saving. """.stripMargin)
    }
  }
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
      .map(value =>
        Try(value.toInt)
          .map(TTLOption.constant)
          .getOrElse(TTLOption.perRow(value)))
      .getOrElse(writeConf.ttl)

  private val timestampWriteOption =
    catalogConf.getOption(WriteTimeParam.name)
      .map(value =>
        Try(value.toLong)
          .map(TimestampOption.constant)
          .getOrElse(TimestampOption.perRow(value)))
      .getOrElse(writeConf.timestamp)

  val metadataEnrichedWriteConf = writeConf.copy(
    ttl = ttlWriteOption,
    timestamp = timestampWriteOption)

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = getWriterFactory()

  override def commit(messages: Array[WriterCommitMessage]): Unit = {} //No Commit in Cassandra Driver Writes

  override def abort(messages: Array[WriterCommitMessage]): Unit = {} //No Abort possible in Cassandra Driver Writes

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = getWriterFactory()

  def getWriterFactory(): CassandraDriverDataWriterFactory = {
    CassandraDriverDataWriterFactory(
      connector,
      tableDef,
      inputSchema,
      metadataEnrichedWriteConf
    )
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
}
