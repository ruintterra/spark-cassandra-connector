package com.datastax.spark.connector.sql

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cluster.TwoClustersWithOneNode
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql.CassandraConnectorConf.{ConnectionHostParam, ConnectionPortParam}
import org.apache.spark.sql.cassandra._

import scala.concurrent.Future

class CassandraSQLClusterLevelSpec extends SparkCassandraITFlatSpecBase with TwoClustersWithOneNode {

  override lazy val conn = CassandraConnector(defaultConf)

  val conf2 = defaultConf
    .set(ConnectionHostParam.name, cluster(1).addresses.head.getAddress.getHostAddress)
    .set(ConnectionPortParam.name, cluster(1).addresses.head.getPort.toString)
  val conn2 = CassandraConnector(conf2)

  private val cluster1 = "cluster1"
  private val cluster2 = "cluster2"

  override def beforeClass {
    awaitAll(
      Future {
        conn.withSessionDo { session =>
          createKeyspace(session, ks)

          session.execute(s"CREATE TABLE $ks.test1 (a INT PRIMARY KEY, b INT, c INT)")
          session.execute(s"INSERT INTO $ks.test1 (a, b, c) VALUES (1, 1, 1)")
          session.execute(s"INSERT INTO $ks.test1 (a, b, c) VALUES (2, 1, 2)")
          session.execute(s"INSERT INTO $ks.test1 (a, b, c) VALUES (3, 1, 3)")
          session.execute(s"INSERT INTO $ks.test1 (a, b, c) VALUES (4, 1, 4)")
          session.execute(s"INSERT INTO $ks.test1 (a, b, c) VALUES (5, 1, 5)")
        }
      },

      Future {
        conn2.withSessionDo { session =>
          createKeyspace(session, ks)

          awaitAll(
            Future {
              session.execute(s"CREATE TABLE $ks.test2 (a INT PRIMARY KEY, d INT, e INT)")
              session.execute(s"INSERT INTO $ks.test2 (a, d, e) VALUES (8, 1, 8)")
              session.execute(s"INSERT INTO $ks.test2 (a, d, e) VALUES (7, 1, 7)")
              session.execute(s"INSERT INTO $ks.test2 (a, d, e) VALUES (6, 1, 6)")
              session.execute(s"INSERT INTO $ks.test2 (a, d, e) VALUES (4, 1, 4)")
              session.execute(s"INSERT INTO $ks.test2 (a, d, e) VALUES (5, 1, 5)")
            },

            Future {
              session.execute(s"CREATE TABLE $ks.test3 (a INT PRIMARY KEY, d INT, e INT)")
            }
          )
        }
      }
    )

    sparkSession.setCassandraConf(cluster1,
      ConnectionHostParam.option(cluster(0).addresses.head.getAddress.getHostAddress) ++ ConnectionPortParam.option(cluster(0).addresses.head.getPort))
    sparkSession.setCassandraConf(cluster2,
      ConnectionHostParam.option(cluster(1).addresses.head.getAddress.getHostAddress) ++ ConnectionPortParam.option(cluster(1).addresses.head.getPort))
  }

  "SqlSession" should "allow to join tables from different clusters" in {
    sparkSession.read.cassandraFormat("test1", ks, cluster1).load().createOrReplaceTempView("c1_test1")
    sparkSession.read.cassandraFormat("test2", ks, cluster2).load().createOrReplaceTempView("c2_test2")

    val result = sparkSession.sql(s"SELECT * FROM c1_test1 AS test1 JOIN c2_test2 AS test2 WHERE test1.a = test2.a").collect()
    result should have length 2
  }

  it should "allow to write data to another cluster" in {
    sparkSession.read.cassandraFormat("test1", ks, cluster1).load().createOrReplaceTempView("c1_test1")
    sparkSession.read.cassandraFormat("test3", ks, cluster2).load().createOrReplaceTempView("c2_test3")

    val insert = sparkSession.sql(s"INSERT INTO TABLE c2_test3 SELECT a, b as d, c as e FROM c1_test1 AS t1").collect()
    val result = sparkSession.sql(s"SELECT * FROM c2_test3 AS test3").collect()
    result should have length 5
  }
}
