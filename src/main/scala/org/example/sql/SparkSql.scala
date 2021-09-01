package org.example.sql

object SparkSql {
  def main(args: Array[String]): Unit = {
    val spark = SparkContext.spark

    val users = spark.read.json("datas/user.json")
    users.createOrReplaceTempView("user")
    users.show()

    val userRoles = spark.read.json("datas/user_role.json")
    userRoles.createOrReplaceTempView("user_role")
    userRoles.show()

    spark.sql("select * from user join user_role on user.id = user_role.user_id").show()
    users.join(userRoles).where("id = user_id").show()

    spark.stop()
  }
}
