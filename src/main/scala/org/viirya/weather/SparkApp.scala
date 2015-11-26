
package org.viirya.weather

import java.io._

import scala.util.Random

import org.apache.log4j.Logger
import org.apache.log4j.Level

import com.datastax.spark.connector._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

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
      case "insert" =>
        val dataDir = parseOptionalArg(args, 4,
          "Directory to data files should be given in action mode.")
        insertWeatherData(dataDir, sc, keyspace, table)
      case "insert-hdfs" =>
        val hdfsDir = parseOptionalArg(args, 4,
          "Directory to data files should be given in action mode.")
        insertWeatherDataFromHDFS(hdfsDir, sc, keyspace, table)
      case "insert-binary-hdfs" =>
        val hdfsDir = parseOptionalArg(args, 4,
          "Directory to data files should be given in action mode.")
        insertBinaryDataFromHDFS(hdfsDir, sc, keyspace, table)
      case "query-hdfs" =>
        val hdfsDir = parseOptionalArg(args, 4,
          "Directory to data files should be given in action mode.")
        val queryNum = parseOptionalArg(args, 5,
          "The number of queries should be given in query mode.")
        queryWeatherDataFromHDFS(hdfsDir, sc, keyspace, table, queryNum.toInt)
      case "update-hdfs" =>
        val hdfsDir = parseOptionalArg(args, 4,
          "Directory to data files should be given in action mode.")
        insertWeatherDataFromHDFS(hdfsDir, sc, keyspace, table, true)
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

  def listHDFSDir(hdfsDir: String): Seq[String] = {
    val conf = new Configuration
    val path = new Path(hdfsDir)
    val fs = path.getFileSystem(conf)
    fs.listStatus(path) match {
      case null => Nil
      case statuses => statuses.map(_.getPath.toString)
    }
  }

  def queryWeatherDataFromHDFS(
      hdfsDir: String,
      sc: SparkContext,
      keyspace: String,
      table: String,
      number: Int) = {
    println(s"Loading data from hdfs dir: $hdfsDir")
    val textRDD = sc.textFile(hdfsDir)
    val recordsRDD = textRDD.flatMap { line =>
      val random = new Random()
      val reader = new WeatherDataReader()
      val parsed = reader.parseWeatherDataWithType(line)
      if (parsed.isDefined) {
        val item = parsed.get
        // (loc)
        Seq((item._1 + item._2), (item._2 + item._1))
      } else {
        Seq()
      }
    }.take(number)
    println(s"RDD count: ${recordsRDD.size}")
    recordsRDD.foreach { pk =>
      sc.cassandraTable(keyspace, table).select("loc", "datestring", "date", "ttype", "temperature")
        .where("loc = ?", pk).toArray.foreach(println)
    }
  }

  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  def insertBinaryDataFromHDFS(
      hdfsDir: String,
      sc: SparkContext,
      keyspace: String,
      table: String,
      update: Boolean = false) = {
    println(s"Loading data from hdfs dir: $hdfsDir")
    val sqlContext = new SQLContext(sc)
    listHDFSDir(hdfsDir).foreach { dir =>
      val binaryRDD = sqlContext.parquetFile(dir).map(serialize(_))
      val recordsRDD = binaryRDD.flatMap { blob =>
        val random = new Random()
        val (pk1, pk2, pk3) = (random.nextInt(), random.nextInt(), random.nextInt())
        Seq((pk1.toString, pk2.toString, pk3.toString, blob),
          (pk3.toString, pk2.toString, pk1.toString, blob))
      }
      println(s"RDD count: ${recordsRDD.count}")
      recordsRDD.saveToCassandra(keyspace, table,
        SomeColumns("pk1", "pk2", "pk3", "data"))
    }
  }

  def insertWeatherDataFromHDFS(
      hdfsDir: String,
      sc: SparkContext,
      keyspace: String,
      table: String,
      update: Boolean = false) = {
    println(s"Loading data from hdfs dir: $hdfsDir")
    val textRDD = sc.textFile(hdfsDir)
    val recordsRDD = textRDD.flatMap { line =>
      val random = new Random()
      val reader = new WeatherDataReader()
      val parsed = reader.parseWeatherDataWithType(line)
      if (parsed.isDefined) {
        val (plus1, plus2) = if (update) {
          (random.nextInt(), random.nextInt())
        } else {
          (0, 0)
        }
        val item = parsed.get
        // (loc + date, date as string, date, ttype, temperature)
        Seq((item._1 + item._2, item._2, item._2, item._3, (item._4.toInt + plus1).toString),
          (item._2 + item._1, item._2, item._2, item._3, (item._4.toInt + plus2).toString))
      } else {
        Seq()
      }
    }
    println(s"RDD count: ${recordsRDD.count}")
    recordsRDD.saveToCassandra(keyspace, table,
      SomeColumns("loc", "datestring", "date", "ttype", "temperature"))
  }

  def insertWeatherData(dataDir: String, sc: SparkContext, keyspace: String, table: String) = {
    val reader = new WeatherDataReader()
    val files = reader.findDataFileInDir(dataDir)
    files.foreach { (f) =>
      println(s"Reading data from file: $f")
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
      println(s"RDD count: ${recordsRDD.count}")
      recordsRDD.saveToCassandra(keyspace, table, SomeColumns("loc", "date", "tmax", "tmin"))
    }
  }
}
