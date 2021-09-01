package org.example.sql

object SparkDataSet {
  def main(args: Array[String]): Unit = {
    val spark = SparkContext.spark
    import spark.implicits._

    val userDataFrame = spark.read.json("datas/user.json")
    userDataFrame.show()

    val userDataSet = userDataFrame.as[User]
    userDataSet.show()
  }
}