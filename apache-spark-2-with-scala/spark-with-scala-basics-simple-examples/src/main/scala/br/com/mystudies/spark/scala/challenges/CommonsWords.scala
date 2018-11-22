package br.com.mystudies.spark.scala.challenges

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object CommonsWords extends App {

    val commonsWorlds = List("you", "of", "a", "the", "your", "to")


      // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

     // Create a SparkContext using the local machine
    new SparkContext("local", "WordCountBetterSorted")
      .textFile(getClass.getResource("/datasets/book.txt").getPath)
      .flatMap(_.split("\\W+"))
      .map(_.toLowerCase())
      .filter(commonsWorlds.contains(_))
      .map((_, 1)).reduceByKey( (x,y) => x + y )
      .map( x => (x._2, x._1))
      .sortByKey()
      .foreach(result => {
        	  val count = result._1
    			  val word = result._2
    			  println(s"$word: $count")
      })
}