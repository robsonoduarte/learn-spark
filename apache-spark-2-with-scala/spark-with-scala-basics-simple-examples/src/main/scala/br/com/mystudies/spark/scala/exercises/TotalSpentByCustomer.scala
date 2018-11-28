package br.com.mystudies.spark.scala.exercises

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object TotalSpentByCustomer extends App {

    def parseLine(line:String) = {
        val fields = line.split(",")
        (fields(0).toInt, fields(2).toFloat)
    }


   Logger.getLogger("org").setLevel(Level.ERROR)

   new SparkContext("local[*]", "TotalSpentByCustomer")
      .textFile(getClass.getResource("/datasets/customer-orders.csv").getPath)
      .map(parseLine)
      .reduceByKey((x,y) => x + y)
      .map(x => (x._2, x._1) )
      .sortByKey()
      .collect()
      .foreach(println(_))
}