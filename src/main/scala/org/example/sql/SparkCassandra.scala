package org.example.sql

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.cassandra.{DataFrameReaderWrapper, DataFrameWriterWrapper}

object SparkCassandra {
  def main(args: Array[String]): Unit = {
    val spark = SparkContext.spark
    spark.conf.set("spark.cassandra.connection.host", "localhost")
    spark.conf.set("spark.cassandra.auth.username", "cassandra")

    val users = spark.read
      .cassandraFormat("user", "spark")
      .load()

    users.explain()
    users.show()

    users.write
      .cassandraFormat("user", "spark")
      .mode(SaveMode.Append)
      .save()
  }
}
