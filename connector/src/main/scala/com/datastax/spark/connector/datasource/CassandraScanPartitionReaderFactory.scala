package com.datastax.spark.connector.datasource

import java.util.OptionalLong

import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.datasource.ScanHelper.CqlQueryParts
import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory}
import com.datastax.spark.connector.rdd.partitioner.{CassandraPartition, DataSizeEstimates}
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.util.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.types.StructType

case class CassandraScanPartitionReaderFactory(
  connector: CassandraConnector,
  tableDef: TableDef,
  schema: StructType,
  readConf: ReadConf,
  queryParts: CqlQueryParts) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val cassandraPartition = partition.asInstanceOf[CassandraPartition[Any, _ <: Token[Any]]]
    CassandraScanPartitionReader(
      connector,
      tableDef,
      schema,
      readConf,
      queryParts,
      cassandraPartition)
  }
}

/**
  * Physical Scan Reader of Cassandra
  *
  * @param connector Connection to Cassandra to use for Reading
  * @param tableDef Table Definition Information for the table being scanned
  * @param schema Output Schema to be produced from this read
  * @param readConf Options relating to how the read should be performed
  * @param queryParts Additional query elements to add to the TokenRange Scan query
  * @param partition The Token Range to Query with Localization Info
  */
case class CassandraScanPartitionReader(
  connector: CassandraConnector,
  tableDef: TableDef,
  schema: StructType,
  readConf: ReadConf,
  queryParts: CqlQueryParts,
  partition: CassandraPartition[Any, _ <: Token[Any]])
  extends PartitionReader[InternalRow]
    with SupportsReportStatistics
    with Logging{

  val tokenRanges = partition.tokenRanges

  val columnNames = queryParts.selectedColumnRefs.map(_.selectedAs).toIndexedSeq
  val rowReader = new UnsafeRowReaderFactory(schema).rowReader(tableDef, queryParts.selectedColumnRefs)
  val scanner = connector.connectionFactory.getScanner(readConf, connector.conf, columnNames)

  /*
  Iterator flatMap trick flattens the iterator-of-iterator structure into a single iterator.
  flatMap on iterator is lazy, therefore a query for the next token range is executed not earlier
  than all of the rows returned by the previous query have been consumed.
  Prepended InternalRow to consume first "Next" call which happens before the first "get" call
  */
  val rowIterator = {
    tokenRanges.iterator.flatMap{ range =>
      val scanResult = ScanHelper.fetchTokenRange(scanner, tableDef,  queryParts, range, readConf.consistencyLevel, readConf.fetchSizeInRows)
      val meta = scanResult.metadata
      scanResult.rows.map(rowReader.read(_, meta))
    }
  }

  var lastRow: InternalRow = InternalRow()

  override def next(): Boolean = {
   if (rowIterator.hasNext) {
       lastRow = rowIterator.next()
     true
   } else {
     false
   }
  }

  override def get(): InternalRow = lastRow

  override def close(): Unit = {
    scanner.close()
  }

  //Currently Does not take into account TODO filters or pruning
  override def estimateStatistics(): Statistics = new Statistics {
    //We call the types from the void
    type V = t forSome { type t }
    type T = t forSome { type t <: Token[V] }

    override def sizeInBytes(): OptionalLong = {
      implicit val tokenFactory = TokenFactory.forSystemLocalPartitioner(connector)
      val sizeInBytes = new DataSizeEstimates[V, T](connector, tableDef.keyspaceName, tableDef.keyspaceName).dataSizeInBytes
      OptionalLong.of(sizeInBytes)
    }

    //We would need to do something much cleverer here to actually get something accurate, todo for later
    override def numRows(): OptionalLong = OptionalLong.empty()
  }

  override def readSchema(): StructType = schema
}
