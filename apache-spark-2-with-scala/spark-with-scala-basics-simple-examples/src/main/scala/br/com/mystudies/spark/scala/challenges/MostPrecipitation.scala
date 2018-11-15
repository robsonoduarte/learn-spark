package br.com.mystudies.spark.scala.challenges

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min


/*
 * what day had the most precipitation for each location that we have precipitation data for and the general
 * concept will be the same for the code you kind of parse out the data a little bit differently and treated
 * a little bit differently right.
 */

object MostPrecipitation extends App {

    def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val date = fields(1)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, date, entryType, temperature)
  }


   Logger.getLogger("org").setLevel(Level.ERROR)

   new SparkContext("local[*]", "MostPrecipitation")
      .textFile(getClass.getResource("/datasets/1800.csv").getPath)
      .map(parseLine)
      .filter(x => x._3 == "PRCP")
      .map(x => (x._1, (x._2, x._4.toFloat)))
      .reduceByKey( (x,y) => if(x._2 > y._2) (x._1, x._2) else (y._1, y._2))
      .foreach(result => {
           val station = result._1
           val date = result._2._1
           val formattedTemp = f"${result._2._2}%.2f F"
         println(s"$station on date $date have this most precipitation: $formattedTemp")
      })

}