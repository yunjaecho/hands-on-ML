package pl.younjae.sparkml

import org.apache.spark.sql.{DataFrame, SQLContext}
import pl.younjae.sparkml.utils.InputCleanUtil

class MysqlLoader(sqlContext: SQLContext, runConfig: RunConfig) {
  def load(): Unit = {
    val dataframe_mysql: DataFrame =
      sqlContext.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost/forum")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "b_messages")
      .option("user", "root")
      .option("password", "wofl07").load()

    val cleanedDf = InputCleanUtil.tokenizeAndStopWords(dataframe_mysql , sqlContext)
  }


}
