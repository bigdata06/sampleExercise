
package org.viirya.weather

import org.scalatest.FunSuite

class WeatherDataReaderSuite extends FunSuite {
 
  val reader = new WeatherDataReader()
  val exampleDataFile = "src/test/resources/test.csv"

  test("read weather data file") {
    val weatherData = reader.readFile(exampleDataFile)
    assert(weatherData(("USC00300379", "1905-01-01")) === Seq(50, 0))
  }

  test("find files in dir") {
    val files = reader.findDataFileInDir("src/test/resources")
    assert(files.contains("test.csv"))
  }
}
