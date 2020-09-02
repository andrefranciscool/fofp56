package example

import org.apache.spark.sql.DataFrame

class writeData(df: DataFrame, args: Array[String]) {

  def writeAnalysisCSV(df: DataFrame): Unit ={
    df.coalesce(1).write.mode("overwrite")
      .option("sep", ";")
      .option("header", "true")
      .option("encoding", "ISO-8859-1")
      .csv(args(3)+args(4) + "generalAnalysis")
  }

  def writeAnalysisJson(df: DataFrame): Unit ={
    df.coalesce(1).write.mode("overwrite")
      .option("sep", ";")
      .option("header", "true")
      .option("encoding", "ISO-8859-1")
      .csv(args(2)+args(3)+ "generalAnalysis")
  }

  def writeAnalysisSQL(df: DataFrame): Unit ={
    df.coalesce(1).write.mode("overwrite")
      .option("sep", ";")
      .option("header", "true")
      .option("encoding", "ISO-8859-1")
      .csv(args(6)+args(7)+ "generalAnalysis")
  }

  def writeAnalysisHIVE(df: DataFrame): Unit ={
    df.coalesce(1).write.mode("overwrite")
      .option("sep", ";")
      .option("header", "true")
      .option("encoding", "ISO-8859-1")
      .csv(args(3)+ args(4)+"generalAnalysis")
  }

}
