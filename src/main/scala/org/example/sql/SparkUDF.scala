package org.example.sql

object SparkUDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkContext.spark

    spark.udf.register("prefix_name", (name: String) => {
      "name:" + name
    })

    val users = spark.read.json("datas/user.json")
    users.createOrReplaceTempView("user")

    spark.sql("select id, prefix_name(name) from user").show()
  }
}
