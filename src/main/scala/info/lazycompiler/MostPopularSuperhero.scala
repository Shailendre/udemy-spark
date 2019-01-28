package main.scala.info.lazycompiler

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MostPopularSuperhero {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc: SparkContext = new SparkContext("local[*]", "MOST-POPULAR-SUPERHERO")

    val namesMap = getSuperHero(sc)
    val connectionRDD = getSuperHeroConnections(sc)

    println(namesMap(connectionRDD(0)._1 - 1))


  }

  def getSuperHero(sc: SparkContext): Array[Any] = {

    val lines = sc.textFile(Utils.resourcePath + "Marvel-names.txt")
    lines.map(l => {
      var sp = l.split("\"")
      if (sp.length >= 2) {
        (sp(0).trim.toInt, sp(1))
      }
    }).collect()
  }

  def getSuperHeroConnections(sc: SparkContext): Array[(Int, Int)] = {
    val lines = sc.textFile(Utils.resourcePath + "Marvel-graph.txt")
    lines
      .map(l => {
        val sp = l.split("\\s+")
        (sp(0).toInt, sp.length - 1)
      })
      .reduceByKey((x, y) => x + y)
      .sortBy(x => x._2, ascending = false)
      .collect()
  }


}
