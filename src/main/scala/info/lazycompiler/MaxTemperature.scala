package main.scala.info.lazycompiler

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.math._

object MaxTemperature {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    // create spark context
    val sc: SparkContext = new SparkContext("local[*]", "MAX_TEMPERATURE")
    // create rdd for 1800 datasets
    val rdd = sc.textFile(Utils.resourcePath + "1800.csv")
    // filter the rdds based in criteria
    val maxTemperatureTuples = rdd
      .map(d => {
        val sp: Array[String] = d.split(",")
        (sp(0), sp(2), sp(3).toFloat)
      })
      .filter(d => d._2 == "TMAX")
      .map(d => (d._1, d._3))
      .reduceByKey((x, y) => max(x, y))

    // print the rdds
    for (t <- maxTemperatureTuples)
      println(t)


  }
}
