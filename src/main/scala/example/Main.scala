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

  }else if (args.length == 5 && (args(0)=="csv" || args(0) == "txt")) {
    val df = new readData(conn).readCsv(args)
    val dataAnalysis= new DataAnalysis(conn, df)
    val write = new writeData(df, args)
    dataAnalysis.createAnalysis()
    write.writeAnalysis(dataAnalysis.createAnalysis())

  }
  else if (args.length == 4 && args(0) =="json"){
    val df = new readData(conn).readJson(args)
    val dataAnalysis= new DataAnalysis(conn, df)
    val write = new writeData(df, args)
    dataAnalysis.createAnalysis()
    write.writeAnalysis(dataAnalysis.createAnalysis())
  }
  else {
    println("Wrong Input")
    println("for csv or txt files you need five parameters. Example <file format> <delimiter> <url server> <source path> <destination path>")
    println("for json files you need four parameters. Example <file format> <url server> <source path> <destination path>")
  }

}



