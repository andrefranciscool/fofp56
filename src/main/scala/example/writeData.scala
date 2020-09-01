package example

import org.apache.spark.sql.DataFrame

class writeData(df: DataFrame, args: Array[String]) {
  def writeAnalysis(df: DataFrame): Unit ={
    df.coalesce(1).write.mode("overwrite")
      .option("sep", ";")
      .option("header", "true")
      .option("encoding", "ISO-8859-1")
      .csv(args(1)+args(3))
  }
}
