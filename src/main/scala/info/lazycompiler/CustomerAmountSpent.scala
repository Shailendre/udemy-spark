package main.scala.info.lazycompiler

object CustomerAmountSpent {

  def main(args: Array[String]): Unit = {

    // init the spark context with file name and context name
    // returns ths rdd[String]
    val rdd = Utils._init("customer-orders.csv", "CUSTOMER-ORDERS")

    val op = rdd
      .map(x => {val split = x.split(","); (split(0), split(2).toFloat)})
      .reduceByKey((x, y) => x + y)
      .sortBy(x => x._2, ascending = false)
      .collect()

    op.foreach(println)



  }



}
