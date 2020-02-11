package pl.younjae.sparkTutorial.rdd

import org.apache.spark.{SparkConf, SparkContext}
import pl.younjae.sparkTutorial.rdd.commons.Utils

object AirportsInUsaSolution {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val airports = sc.textFile("in/airports.text")
    val airportsInUSA = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(3) == "\"United States\"")

    val airportsNameAndCityNames = airportsInUSA.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(2)
    })
    airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa.text")
  }
}
