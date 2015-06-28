
package org.viirya.weather

import org.apache.log4j.Logger
import org.apache.log4j.Level

import com.datastax.spark.connector._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel

object SparkApp {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    if (args.length < 4) {
      println("Usage: SparkApp <cassandra host> <cassandra keyspace> <table> <action> <arg>")
      println("<action> could be insert, analysis.")
      System.exit(0)
    }

    val cassandraHost = args(0)
    val keyspace = args(1)
    val table = args(2)
    val action = args(3)

    val conf = new SparkConf().setAppName("SparkApp")
      .set("spark.cassandra.connection.host", cassandraHost)
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("rdd_checkpoint")

    action match {
      case "test" =>
        val rdd = sc.cassandraTable(keyspace, table)
        println(rdd.count)
        println(rdd.first)
        println(rdd.map(_.getInt("value")).sum)       
      case "insert" =>
        val dataDir = parseOptionalArg(args, 4,
          "Directory to data files should be given in action mode.")
        insertWeatherData(dataDir, sc, keyspace, table)
      case "analysis" =>
        val year = parseOptionalArg(args, 4, "Year should be given in analysis mode.")
        val rdd = sc.cassandraTable(keyspace, table).select("tmax", "tmin")
          .where("date >= ?", s"$year-01-01").where("date <= ?", s"$year-12-31")
        val maxTemperature = rdd.map(_.getInt("tmax")).toArray
        val minTemperature = rdd.map(_.getInt("tmin")).toArray
        val avgMaxTemperature = getAvgTemperature(maxTemperature)
        val avgMinTemperature = getAvgTemperature(minTemperature)
        println("Average maximum and minimum temperature in " +
          s"$year: $avgMaxTemperature, $avgMinTemperature")
      case _ =>
        throw new IllegalArgumentException("Unknown action.")
    }
 
    sc.stop()
  }

  def getAvgTemperature(temperature: Seq[Int]): Double = {
    if (temperature.length > 0) {
      temperature.sum / temperature.length.toDouble
    } else {
      0.0
    }
  }

  def parseOptionalArg(args: Array[String], argIndex: Int, errorMsg: String): String = {
    if (args.length >= argIndex + 1) {
      args(argIndex)
    } else {
      throw new IllegalArgumentException(errorMsg)
    }
  }

  def insertWeatherData(dataDir: String, sc: SparkContext, keyspace: String, table: String) = {
    val reader = new WeatherDataReader()
    val files = reader.findDataFileInDir(dataDir)
    files.foreach { (f) =>
      val weatherData = reader.readFile(dataDir + "/" + f)
      val records = weatherData.flatMap { (item) =>
        if (item._2.length == 2) {
          // (loc, date, tmax, tmin)
          Seq((item._1._1, item._1._2, item._2(0), item._2(1)))
        } else {
          Seq()
        }
      }.toSeq
      val recordsRDD = sc.parallelize(records)
      recordsRDD.saveToCassandra(keyspace, table, SomeColumns("loc", "date", "tmax", "tmin"))
    }
  }
}
