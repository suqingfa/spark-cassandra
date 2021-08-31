package org.example

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    // spark config
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("WordCount")

    // spark context
    val context = new SparkContext(sparkConf)

    context
      .textFile("datas") // read txt
      .flatMap(_.split(" ")) // split
      .map((_, 1)) // map
      .reduceByKey(_ + _) // add
      .foreach(println) // print

    context.stop()
  }
}
