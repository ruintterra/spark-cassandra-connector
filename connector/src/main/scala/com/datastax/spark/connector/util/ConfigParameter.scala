package com.datastax.spark.connector.util

import com.datastax.spark.connector.cql.{AuthConfFactory, CassandraConnectionFactory, CassandraConnectorConf}
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.types.ColumnTypeConf
import com.datastax.spark.connector.writer.WriteConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSourceRelation

class ConfigParameter[T] private (
  val name: String,
  val section: String,
  val default: T,
  val description: String) extends DataFrameOption with Serializable {

  override val sqlOptionName = name.replaceAll("\\.", "\\_")

  def option(value: Any): Map[String, String] = {
    require(value != null)
    Map(name -> value.toString)
  }
}

/**
  * Class representing a Config Parameter no longer in use and it's replacement if any. Use
  * rational to display more information about the deprecation and what to do for the end user.
  */
class DeprecatedConfigParameter[N] private (
  val name: String,
  val replacementParameter: Option[ConfigParameter[N]],
  val replacementMethod: (String => String) = identity,
  val deprecatedSince: String,
  val rational: String = "") extends Logging with Serializable {

  def explanation: String = replacementParameter match {
    case Some(replacement) => s"$name is deprecated ($deprecatedSince) and has been automatically replaced with parameter ${replacement.name}. $rational"
    case None => s"$name is deprecated ($deprecatedSince) and is no longer in use. $rational"
  }

  /**
    * Replaces the deprecated parameter in the Spark Context if it is set, Prefix is used
    * for the Dataframe Syntax which prefixes parameters with Cluster level information
    */
  def maybeReplace(sparkConf: SparkConf): Unit = {
    sparkConf.getOption(name) match {
      case Some(value) => {
        replacementParameter match {
          case Some(replacement) =>
            sparkConf.remove(name)
            sparkConf.set(replacement.name, replacementMethod(value))
            logWarning(explanation)
          case None =>
            throw new IllegalArgumentException(explanation)
        }
      }
      case None =>
    }
  }


}

object ConfigParameter{
  val staticParameters = scala.collection.mutable.Set.empty[ConfigParameter[_]]

  /** Force creation of all Parameters. Any New Parameter Holding Objects should be added here **/
  val ConfObjects = (
    WriteConf,
    ReadConf,
    ColumnTypeConf,
    CassandraSourceRelation,
    CassandraConnectorConf,
    AuthConfFactory,
    CassandraConnectionFactory)

  def names: Seq[String] = staticParameters.map(_.name).toSeq

  def apply[T](name: String, section: String, default: T, description: String): ConfigParameter[T] = {
    val param = new ConfigParameter(name,section,default, description)
    staticParameters.add(param)
    param
  }
}

object DeprecatedConfigParameter {

  val deprecatedParameters = scala.collection.mutable.Set.empty[DeprecatedConfigParameter[_]]
  def names: Seq[String] = deprecatedParameters.map(_.name).toSeq

  def apply[N](
    name: String,
    replacementParameter: Option[ConfigParameter[N]],
    replacementMethod: (String => String) = identity,
    deprecatedSince: String,
    rational: String = ""): DeprecatedConfigParameter[N] = {
    val param = new DeprecatedConfigParameter(name, replacementParameter, replacementMethod, deprecatedSince, rational)
    deprecatedParameters.add(param)
    param
  }
}
