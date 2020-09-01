package example

import org.apache.spark.sql.DataFrame

class writeData(df: DataFrame, args: Array[String]) {
  def writeAnalysisCSV(df: DataFrame): Unit ={
    df.coalesce(1).write.mode("overwrite")
      .option("sep", ";")
      .option("header", "true")
      .option("encoding", "ISO-8859-1")
      .csv(args(2)+args(4) + "generalAnalysis")
  }

  def writeAnalysisJson(df: DataFrame): Unit ={
    df.coalesce(1).write.mode("overwrite")
      .option("sep", ";")
      .option("header", "true")
      .option("encoding", "ISO-8859-1")
      .csv(args(1)+args(3))
  }
}
