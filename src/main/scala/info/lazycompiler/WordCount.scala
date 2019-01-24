package main.scala.info.lazycompiler

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object WordCount {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);

    val sc: SparkContext = new SparkContext("local[*]", "WORD_COUNT")
    val lines: RDD[String] = sc.textFile(Utils.resourcePath + "book.txt")
    val wc = lines
      .flatMap(l => l.split("\\W+"))
      .map(l => {
        var lc = l.toLowerCase;
        (lc, 1)
      })
      .reduceByKey((x, y) => x + y)
      .map(x => (x._2, x._1))
      .sortByKey().collect()

    wc.foreach(x => println(s"(${x._2}, ${x._1})"))

  }


}
