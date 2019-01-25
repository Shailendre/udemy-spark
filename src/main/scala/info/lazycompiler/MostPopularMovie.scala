package main.scala.info.lazycompiler

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

object MostPopularMovie {

  // returns the map of id -> name
  def getItemMap: Map[Int, String] = {

    // there is issue with the encoding of the file
    // else this is not required
    implicit val codec: Codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var map:Map[Int, String] = Map()
    val lines:Iterator[String] = Source.fromFile(Utils.resourcePath + "u.item").getLines()
    lines.foreach(l => {
      var sp = l.split('|')
      map += (sp(0).toInt -> sp(1))
    })
    map
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc:SparkContext = new SparkContext("local[*]", "MOST_POPULAR_MOVIE")

    val rdd = Utils._init("u.data", sc)
    // broadcast the map
    // broadcasting a value is a special concept in spark that sends the data to different clusters so that
    // each cluster has their own copy when needed
    // this concept also handles
    // "to big a value being hold up in memory"
    val itemMap = sc.broadcast(getItemMap)

    val op = rdd
      .map(l => {
        val sp = l.split("\\W+")
        (sp(1).toInt, 1)
      })
      .reduceByKey((x, y) => x + y)
      // map the movie to its name from the broadcast value
      .map(x => (itemMap.value(x._1), x._2))
      .sortBy(x => x._2, ascending = false)
      .collect()


    op.foreach(println)

  }

}
