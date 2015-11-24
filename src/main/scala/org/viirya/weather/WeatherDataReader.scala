package org.viirya.weather
 
import java.io.{File, FileInputStream, DataInputStream, BufferedReader, InputStreamReader}

import scala.collection.mutable
 
class WeatherDataReader {
 
  val locIndex: Int = 0
  val dateIndex: Int = 1
  val typeIndex: Int = 2

  val sepChar: String = ","
  val filePostfix = "csv"

  /**
   *
   * @param dir the directory to weather data files.
   * @return A sequence of string that each is a path to a weather data file.
   */
  def findDataFileInDir(dir: String): Seq[String] = {
    val d = new File(dir)
    d.listFiles.map(_.getName).filter(_.endsWith(filePostfix)).toSeq
  }

  /**
   *
   * @param path the path to the weather data file.
   * @return A map of weather records in which the key is (location, date) and the value is the
   *         sequence of maximum and minimum temperature.
   */
  def readFile(path: String): Map[(String, String), Seq[Int]] = {
    var records = mutable.Seq[(String, String, String)]()
 
    try {
      val fstream = new FileInputStream(path)
      val in = new DataInputStream(fstream)
      val br = new BufferedReader(new InputStreamReader(in))
      var line: String = null
      line = br.readLine()
      while (line != null)   {
        val parsed = parseWeatherData(line)
        if (parsed.isDefined) {
          records = records.+:(parsed.get)
        }
        line = br.readLine()
      }
      in.close()
    } catch {
      case e: Exception =>
        println(e.getMessage())
    }
    records.groupBy((r => (r._1, r._2))).mapValues(s => s.map(_._3.toInt).sorted.reverse)
  } 

  def parseDate(date: String): String = {
    s"${date.substring(0, 4)}-${date.substring(4, 6)}-${date.substring(6, 8)}"
  }

  def parseWeatherDataWithType(line: String): Option[(String, String, String, String)] = {
    val fields = line.split(sepChar)
    if (fields.length >= 4) {
      val loc = fields(locIndex)
      val date = parseDate(fields(dateIndex))

      fields(typeIndex) match {
        case "TMAX" => Some((loc, date, "TMAX", fields(typeIndex + 1)))
        case "TMIN" => Some((loc, date, "TMIN", fields(typeIndex + 1)))
        case _ => None
      }
    } else {
      None
    }
  }

  private def parseWeatherData(line: String): Option[(String, String, String)] = {
    val fields = line.split(sepChar)
    if (fields.length >= 4) {
      val loc = fields(locIndex)
      val date = parseDate(fields(dateIndex))

      fields(typeIndex) match {
        case "TMAX" => Some((loc, date, fields(typeIndex + 1)))
        case "TMIN" => Some((loc, date, fields(typeIndex + 1)))
        case _ => None
      }
    } else {
      None
    }
  }
}
 
