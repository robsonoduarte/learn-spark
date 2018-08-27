package br.com.mystudies.spark.scala

import scala.io.Source

object RatingsCounterScala extends App {
    Source.fromFile("F:/spark-scala-course/ml-100k/u.data")
      .getLines()
      .map(_.split("\t")(2))
      .toSeq
      .groupBy(id => id)
      .mapValues(_.size)
      .toSeq
      .sortBy(_._1)
      .foreach(println)
}