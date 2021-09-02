package org.example.sql

import org.apache.spark.sql.SaveMode

/**
 * spark.load/dataSet.write
 * csv jdbc json orc textFile parquet ...
 */
object SparkLoadSave {
  def main(args: Array[String]): Unit = {
    val spark = SparkContext.spark

    spark.read.json("datas/user.json").show()

    val users = spark.sql("select * from json.`datas/user.json`")

    users.write.mode(SaveMode.Overwrite).json("datas/save")
  }
}
