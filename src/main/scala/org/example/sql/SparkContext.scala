package org.example.sql

import org.apache.spark.sql.SparkSession

object SparkContext {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName(this.getClass.getSimpleName)
    .getOrCreate()
}
