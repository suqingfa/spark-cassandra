package org.example.sql

object SparkDSL {
  def main(args: Array[String]): Unit = {
    val spark = SparkContext.spark

    val users = spark.read.json("datas/user.json")
    users.printSchema()
    users.select("name").withColumnRenamed("name", "username").show()
    users.selectExpr("age + 1").show()
  }
}
