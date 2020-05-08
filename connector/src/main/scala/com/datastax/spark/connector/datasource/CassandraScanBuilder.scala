package com.datastax.spark.connector.datasource

import java.net.InetAddress
import java.util.UUID

import com.datastax.spark.connector.{ColumnName, ColumnRef, RowCountRef, TTL, WriteTime}
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.datasource.CassandraSourceUtil.consolidateConfs
import com.datastax.spark.connector.datasource.ScanHelper.CqlQueryParts
import com.datastax.spark.connector.rdd.{CqlWhereClause, ReadConf}
import com.datastax.spark.connector.types.{InetType, UUIDType, VarIntType}
import com.datastax.spark.connector.util.Quote.quote
import com.datastax.spark.connector.util.{Logging, ReflectionUtil}
import org.apache.spark.sql.cassandra.CassandraSourceRelation.{AdditionalCassandraPushDownRulesParam, TTLParam, WriteTimeParam}
import org.apache.spark.sql.cassandra.{AnalyzedPredicates, Auto, BasicCassandraPredicatePushDown, CassandraPredicateRules, CassandraSQLRow, CassandraSourceRelation, DataTypeConverter, DsePredicateRules, DseSearchOptimizationSetting, InClausePredicateRules, Off, On, SolrConstants, SolrPredicateRules}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, sources}
import org.apache.spark.sql.connector.read.partitioning.{ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsReportPartitioning}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{ArrayType, Decimal, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

case class CassandraScanBuilder (
  session: SparkSession,
  catalogConf: SparkConf,
  tableDef: TableDef,
  catalogName: String,
  options: CaseInsensitiveStringMap)

  extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with Logging{

  val connectorConf = consolidateConfs(catalogConf, options.asCaseSensitiveMap().asScala.toMap, catalogName, tableDef.keyspaceName)
  val connector = CassandraConnector(connectorConf)
  val readConf = ReadConf.fromSparkConf(connectorConf)

  var filtersForCassandra = Array.empty[Filter]
  var selectedColumns: IndexedSeq[ColumnRef] = tableDef.columns.map(_.ref)

  def searchOptimization(): DseSearchOptimizationSetting =
    catalogConf.get(
      CassandraSourceRelation.SearchPredicateOptimizationParam.name,
      CassandraSourceRelation.SearchPredicateOptimizationParam.default
    ).toLowerCase match {
      case "auto" => Auto(catalogConf.getDouble(
        CassandraSourceRelation.SearchPredicateOptimizationRatioParam.name,
        CassandraSourceRelation.SearchPredicateOptimizationRatioParam.default))
      case "on" | "true" => On
      case "off" | "false" => Off
      case unknown => throw new IllegalArgumentException(
        s"""
           |Attempted to set ${CassandraSourceRelation.SearchPredicateOptimizationParam.name} to
           |$unknown which is invalid. Acceptable values are: auto, on, and off
           """.stripMargin)
    }

  def additionalRules(): Seq[CassandraPredicateRules] = {
    connectorConf.getOption(AdditionalCassandraPushDownRulesParam.name)
    match {
      case Some(classes) =>
        classes
          .trim
          .split("""\s*,\s*""")
          .map(ReflectionUtil.findGlobalObject[CassandraPredicateRules])
      case None => AdditionalCassandraPushDownRulesParam.default
    }
  }

  def containsMetaDataRequests(): Unit = {
    (catalogConf.getAllWithPrefix(WriteTimeParam.name + ".") ++
        catalogConf.getAllWithPrefix(TTLParam.name + ".")).nonEmpty
  }

  private def solrPredicateRules: Option[CassandraPredicateRules] = {
    if (searchOptimization.enabled) {
      logDebug(s"Search Optimization Enabled - $searchOptimization")
      Some(new SolrPredicateRules(searchOptimization))
    } else {
      None
    }
  }


  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    logDebug(s"Input Predicates: [${filters.mkString(", ")}]")

    val pv = connector.withSessionDo(_.getContext.getProtocolVersion)

    /** Apply built in rules **/
    val bcpp = new BasicCassandraPredicatePushDown(filters.toSet, tableDef, pv)
    val basicPushdown = AnalyzedPredicates(bcpp.predicatesToPushDown, bcpp.predicatesToPreserve)

    logDebug(s"Basic Rules Applied:\n$basicPushdown")

    val predicatePushDownRules = Seq(
      DsePredicateRules,
      InClausePredicateRules) ++
      solrPredicateRules ++
      additionalRules

    /** Apply non-basic rules **/
    val finalPushdown = predicatePushDownRules.foldLeft(basicPushdown)(
      (pushdowns, rules) => {
        val pd = rules(pushdowns, tableDef, catalogConf)
        logDebug(s"Applied ${rules.getClass.getSimpleName} Pushdown Filters:\n$pd")
        pd
      }
    )
    logDebug(s"Final Pushdown filters:\n$finalPushdown")

    filtersForCassandra = finalPushdown.handledByCassandra.toArray
    finalPushdown.handledBySpark.toArray
  }

  override def pushedFilters(): Array[Filter] = filtersForCassandra

  override def pruneColumns(requiredSchema: StructType): Unit = {
    selectedColumns = requiredSchema.fieldNames.map(tableDef.columnByName).map(_.ref)
  }

  val tableIsSolrIndexed =
    tableDef
      .indexes
      .exists(index => index.className.contains(SolrConstants.DseSolrIndexClassName))

  def getQueryParts(): CqlQueryParts = {
    //Get all required ColumnRefs, MetaDataRefs should be picked out of the ReadColumnsMap
    val requiredCassandraColumns = selectedColumns.map(columnName =>
      metadataReadColumnsMap.getOrElse(columnName.columnName, columnName)
    )

    val solrCountEnabled = searchOptimization().enabled && tableIsSolrIndexed && cqlWhereClause.predicates.isEmpty
    val solrCountWhere = CqlWhereClause(Seq(s"${SolrConstants.SolrQuery} = '*:*'"), Seq.empty)

    if (requiredCassandraColumns.isEmpty){
      //Count Pushdown
      CqlQueryParts(IndexedSeq(RowCountRef),
        if (solrCountEnabled) cqlWhereClause and solrCountWhere else cqlWhereClause,
      None,
      None)

    } else {
      //No Count Pushdown
      CqlQueryParts(requiredCassandraColumns, cqlWhereClause, None, None)
    }
  }

  //Metadata Read Fields
  // ignore case
  val regularColumnNames = tableDef.regularColumns.map(_.columnName.toLowerCase())
  val nonRegularColumnNames = (tableDef.clusteringColumns ++ tableDef.partitionKey).map(_.columnName.toLowerCase)
  val ignoreMissingMetadataColumns: Boolean = catalogConf.getBoolean(CassandraSourceRelation.IgnoreMissingMetaColumns.name,
    CassandraSourceRelation.IgnoreMissingMetaColumns.default)

  def checkMetadataColumn(columnName: String, function: String): Boolean = {
    val lowerCaseName = columnName.toLowerCase

    def metadataError(errorType: String) = {
      throw new IllegalArgumentException(s"Cannot lookup $function on $errorType column $columnName")
    }

    if (nonRegularColumnNames.contains(lowerCaseName)) metadataError("non-regular")
    if (regularColumnNames.contains(lowerCaseName)) true
    else if (ignoreMissingMetadataColumns) false
    else metadataError("missing")
  }

  private val writeTimeFields =
    catalogConf
      .getAllWithPrefix(WriteTimeParam.name + ".")
      .filter { case (columnName: String, writeTimeName: String) => checkMetadataColumn(columnName, "writetime") }
      .map { case (columnName: String, writeTimeName: String) =>
        val colDef = tableDef.columnByNameIgnoreCase(columnName)
        val colType = if (colDef.isMultiCell)
          ArrayType(LongType) else LongType
        (colDef.columnName, StructField(writeTimeName, colType, nullable = false))
      }

  private val ttlFields =
    catalogConf
      .getAllWithPrefix(TTLParam.name + ".")
      .filter { case (columnName: String, writeTimeName: String) => checkMetadataColumn(columnName, "ttl") }
      .map { case (columnName: String, ttlName: String) =>
        val colDef = tableDef.columnByNameIgnoreCase(columnName)
        val colType = if (colDef.isMultiCell)
          ArrayType(IntegerType) else IntegerType
        (colDef.columnName, StructField(ttlName, colType, nullable = true))
      }


  private def metadataReadColumnsMap =
    (ttlFields.map(field => (field._2.name, TTL(field._1, Some(field._2.name)))) ++
      writeTimeFields.map(field => (field._2.name, WriteTime(field._1, Some(field._2.name))))).toMap

  /** Construct Cql clause and retrieve the values from filter */
  private def filterToCqlAndValue(filter: Any): (String, Seq[Any]) = {
    filter match {
      case sources.EqualTo(attribute, value) => (s"${quote(attribute)} = ?", Seq(toCqlValue(attribute, value)))
      case sources.LessThan(attribute, value) => (s"${quote(attribute)} < ?", Seq(toCqlValue(attribute, value)))
      case sources.LessThanOrEqual(attribute, value) => (s"${quote(attribute)} <= ?", Seq(toCqlValue(attribute, value)))
      case sources.GreaterThan(attribute, value) => (s"${quote(attribute)} > ?", Seq(toCqlValue(attribute, value)))
      case sources.GreaterThanOrEqual(attribute, value) => (s"${quote(attribute)} >= ?", Seq(toCqlValue(attribute, value)))
      case sources.In(attribute, values) =>
        (quote(attribute) + " IN " + values.map(_ => "?").mkString("(", ", ", ")"), toCqlValues(attribute, values))
      case _ =>
        throw new UnsupportedOperationException(
          s"It's not a valid filter $filter to be pushed down, only >, <, >=, <= and In are allowed.")
    }
  }

  private def toCqlValues(columnName: String, values: Array[Any]): Seq[Any] = {
    values.map(toCqlValue(columnName, _)).toSeq
  }

  /** If column is VarInt column, convert data to BigInteger */
  private def toCqlValue(columnName: String, value: Any): Any = {
    value match {
      case decimal: Decimal =>
        val isVarIntColumn = tableDef.columnByName(columnName).columnType == VarIntType
        if (isVarIntColumn) decimal.toJavaBigDecimal.toBigInteger else decimal
      case utf8String: UTF8String =>
        val columnType = tableDef.columnByName(columnName).columnType
        if (columnType == InetType) {
          InetAddress.getByName(utf8String.toString)
        } else if (columnType == UUIDType) {
          UUID.fromString(utf8String.toString)
        } else {
          utf8String
        }
      case other => other
    }
  }

  /** Construct where clause from pushdown filters */
  def cqlWhereClause = pushedFilters.foldLeft(CqlWhereClause.empty) { case (where, filter) => {
      val (predicate, values) = filterToCqlAndValue(filter)
      val newClause = CqlWhereClause(Seq(predicate), values)
      where and newClause
    }
  }

  override def build(): Scan = {

    val queryParts = getQueryParts()
    val fields: Seq[StructField] = queryParts.selectedColumnRefs.collect(
      {
        case ColumnName(name, _) => tableDef.columnByName(name)
      }).map(DataTypeConverter.toStructField) ++
      writeTimeFields.map(_._2) ++
      ttlFields.map(_._2)

    val readStruct = StructType(fields)

    CassandraScan(session, connector, tableDef, getQueryParts(), readStruct ,readConf, catalogConf)
  }

}

case class CassandraScan(
  session: SparkSession,
  connector: CassandraConnector,
  tableDef: TableDef,
  cqlQueryParts: CqlQueryParts,
  readSchema: StructType,
  readConf: ReadConf,
  catalogConf: SparkConf) extends Scan
    with Batch
    with SupportsReportPartitioning {



  override def toBatch: Batch = this

  val partitionGenerator = ScanHelper.getPartitionGenerator(
    connector,
    tableDef,
    cqlQueryParts.whereClause,
    session.sparkContext.defaultParallelism * 2 + 1,
    readConf.splitCount,
    readConf.splitSizeInMB * 1024L * 1024L)

  lazy val inputPartitions = partitionGenerator.getInputPartitions()

  override def planInputPartitions(): Array[InputPartition] = {
    inputPartitions
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    CassandraScanPartitionReaderFactory(connector, tableDef, readSchema, readConf, cqlQueryParts)
  }

  val clusteredKeys = tableDef.partitionKey.map(_.columnName).toArray

  override def outputPartitioning(): Partitioning = {
    new Partitioning {
      override def numPartitions(): Int = inputPartitions.length

      /*
        Currently we only satisfy distributions which rely on all partition key values having identical
        values. In the future we may be able to support some other distributions but Spark doesn't have
        means to support those atm 3.0
      */

      override def satisfy(distribution: Distribution): Boolean = distribution match {
        case cD: ClusteredDistribution => clusteredKeys.forall(cD.clusteredColumns.contains)
        case _ => false
      }
    }
  }

  override def description(): String = {
    s"""Cassandra Scan ${tableDef.keyspaceName}.${tableDef.tableName} |
       |Server Side Filters ${cqlQueryParts.whereClause} |
       | Columns ${cqlQueryParts.selectedColumnRefs.mkString("[", ",", "]")}""".stripMargin

  }
}

