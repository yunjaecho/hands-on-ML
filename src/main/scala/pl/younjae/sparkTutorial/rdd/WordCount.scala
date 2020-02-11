package pl.younjae.sparkTutorial.rdd

import org.apache.spark.{SparkConf, SparkContext}


object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")

    val words = lines.flatMap(_.split(" "))

    val wordCounts = words.countByValue()
    for((word, count) <- wordCounts) println(word + " : " + count)
  }
}
