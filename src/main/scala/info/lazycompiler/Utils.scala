package main.scala.info.lazycompiler

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Utils {

  val resourcePath: String = "C:\\Users\\ShailendraS\\code\\udemy-spark-scala\\src\\main\\resources\\";

  def _init(file: String, contextName: String): RDD[String] = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    val sc: SparkContext = new SparkContext("local[*]", contextName)
    sc.textFile(resourcePath + file)
  }

}
