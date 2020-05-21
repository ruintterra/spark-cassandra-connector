package com.datastax.spark.connector.datasource

import java.util.OptionalLong

import com.datastax.spark.connector.RowCountRef
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.datasource.ScanHelper.CqlQueryParts
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.rdd.partitioner.dht.{Token, TokenFactory}
import com.datastax.spark.connector.rdd.partitioner.{CassandraPartition, DataSizeEstimates}
import com.datastax.spark.connector.util.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.{LongType, StructField, StructType}

case class CassandraScanPartitionReaderFactory(
  connector: CassandraConnector,
  tableDef: TableDef,
  schema: StructType,
  readConf: ReadConf,
  queryParts: CqlQueryParts) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {

    val cassandraPartition = partition.asInstanceOf[CassandraPartition[Any, _ <: Token[Any]]]
    if (queryParts.selectedColumnRefs.contains(RowCountRef)) {
      //Count Pushdown
      CassandraCountPartitionReader(
        connector,
        tableDef,
        schema,
        readConf,
        queryParts,
        cassandraPartition)
    } else {
      CassandraScanPartitionReader(
        connector,
        tableDef,
        schema,
        readConf,
        queryParts,
        cassandraPartition)
    }
  }
}



abstract class CassandraPartitionReaderBase
  extends PartitionReader[InternalRow]
    with SupportsReportStatistics
    with Logging{

  val connector: CassandraConnector
  val tableDef: TableDef
  val schema: StructType
  val readConf: ReadConf
  val queryParts: CqlQueryParts
  val partition: CassandraPartition[Any, _<: Token[Any]]

  val tokenRanges = partition.tokenRanges

  def columnNames = queryParts.selectedColumnRefs.map(_.selectedAs).toIndexedSeq
  def rowReader = new UnsafeRowReaderFactory(schema).rowReader(tableDef, queryParts.selectedColumnRefs)
  def scanner = connector.connectionFactory.getScanner(readConf, connector.conf, columnNames)

  /*
  Iterator flatMap trick flattens the iterator-of-iterator structure into a single iterator.
  flatMap on iterator is lazy, therefore a query for the next token range is executed not earlier
  than all of the rows returned by the previous query have been consumed.
  */
  val rowIterator = getIterator()

  protected def getIterator(): Iterator[InternalRow]= {
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
  partition: CassandraPartition[Any, _ <: Token[Any]]) extends CassandraPartitionReaderBase

/**
  * Runs a COUNT(*) query instead of a request for actual rows
  * Takes the results and returns that many empty internal rows
  */
case class CassandraCountPartitionReader(
  connector: CassandraConnector,
  tableDef: TableDef,
  schema: StructType,
  readConf: ReadConf,
  queryParts: CqlQueryParts,
  partition: CassandraPartition[Any, _ <: Token[Any]]) extends CassandraPartitionReaderBase {

  //Our read is not based on the structure of the table we are reading from
  override val rowReader = new UnsafeRowReaderFactory(StructType(Seq(StructField("count", LongType, false))))
    .rowReader(tableDef, queryParts.selectedColumnRefs)

  override val rowIterator: Iterator[InternalRow] = {
    val count = getIterator().foldLeft(0L){ case (count, result) => count + result.getLong(0)}

    //Casting issue here for extremely large C* partitions, but it's un likely that a Count Request will succeed if the
    //Split has more than Int.Max Entries anyway.
    //TODO combine iterators to support full Long values
    Iterator.fill(count.toInt)(InternalRow.empty)
  }
}

