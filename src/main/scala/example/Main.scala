package example
import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

object Main extends App{
  val conn = new Connections()
  //val readData = new readData(conn)
  //val df = readData.df
  if (args.length == 0) {
    println("dude, i need at least one parameter")
  }else {
    val df = read(args(0))
    val dataAnalysis= new DataAnalysis(conn, df)
    val write = new writeData(df, args)
    dataAnalysis.createAnalysis()
    write.writeAnalysis(dataAnalysis.createAnalysis())

  }
  def read(x: String): DataFrame = x match {
    case "csv" => val df = new readData(conn).readCsv(args)
      return df
    case "txt" => val df = new readData(conn).readCsv(args)
      return df
    case "json" => val df = new readData(conn).readJson(args)
      return df
  }


}



