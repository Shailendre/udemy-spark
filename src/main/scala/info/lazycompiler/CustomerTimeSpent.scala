package main.scala.info.lazycompiler

object CustomerTimeSpent {

  def main(args: Array[String]): Unit = {

    // init the spark context with file name and context name
    // returns ths rdd[String]
    val rdd = Utils._init("customer-orders.csv", "CUSTOMER-ORDERS")

    val op = rdd
      .map(x => {val split = x.split(","); (split(0), split(2).toFloat)})
      .reduceByKey((x, y) => x + y)
      .collect()

    op.foreach(println)



  }



}
