package org.example.sql

object SparkWindowFunction {
  def main(args: Array[String]): Unit = {
    val spark = SparkContext.spark
    import spark.implicits._

    val df = Seq(
      (1, "2021-01-01", 1000),
      (1, "2021-01-02", 1010),
      (1, "2021-01-03", 1005),
      (1, "2021-01-05", 999),
      (2, "2021-01-01", 2000),
      (2, "2021-01-02", 2100),
      (2, "2021-01-03", 2200),
      (2, "2021-01-06", 1900),
    ).toDF("id", "trading_day", "close")

    df.createOrReplaceTempView("stock_quote")

    spark.sql("select * from stock_quote").show()

    spark.sql(
      """
        |select *,
        |       min(close) over (partition by id order by trading_day rows between 1 preceding and 1 preceding) as pre_close
        |from stock_quote
        |""".stripMargin).show()

    val dataFrame = spark.sql(
      """
        |select *, (close - pre_close) / pre_close as yield
        |from (
        |         select *,
        |                min(close) over (partition by id order by trading_day rows between 1 preceding and 1 preceding) as pre_close
        |         from stock_quote
        |     ) as quote
        |""".stripMargin)
    dataFrame.show()
  }
}
