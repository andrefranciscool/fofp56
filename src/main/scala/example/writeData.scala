package example

import org.apache.spark.sql.DataFrame

class writeData(df: DataFrame, args: Array[String]) {

  def writeAnalysisCSV(df: DataFrame): Unit ={
    try{
      df.coalesce(1).write.mode("overwrite")
        .option("sep", ";")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .csv(args(3)+args(4) + "generalAnalysis")
    }
    catch {
      case x : Exception => println("Destination Path already exists. Please choose another one")
    }
  }

  def writeAnalysisJson(df: DataFrame): Unit ={
    try {
      df.coalesce(1).write
        .option("sep", ";")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .csv(args(2)+args(3)+ "generalAnalysis")
    }
    catch {
      case x : Exception => println("Destination Path already exists. Please choose another one")
    }
  }

  def writeAnalysisSQL(df: DataFrame): Unit ={
    try{
      df.coalesce(1).write.mode("overwrite")
        .option("sep", ";")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .csv(args(6)+args(7)+ "generalAnalysis")
    }
    catch {
      case x : Exception => println("Destination Path already exists. Please choose another one")
    }
  }

  def writeAnalysisHIVE(df: DataFrame): Unit ={
    try{
      df.coalesce(1).write.mode("overwrite")
        .option("sep", ";")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .csv(args(3)+ args(4)+"generalAnalysis")
    }
    catch {
      case x : Exception => println("Destination Path already exists. Please choose another one")
    }
  }

}
