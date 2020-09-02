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
    println("""
      ****************     ****************     ****************
      ****************     ****************     ****************
      ****        ****     ********             *****
      ****        ****     ********             *****
      ****************     ****************     ****************
      ****************             ********     ****************
      ****                         ********     *****     ******
      ****                 ****************     *****     ******
      ****                 ****************     ****************
      ****                 ****************     ****************
      ___________________________________________________________

               Automated Script for Data Quality Analysis

               Version: 1.0.1
               Credits: André Leite
                        Inês Machado
                        Maria Cardoso
                        Rui Faria
      ___________________________________________________________
    """)
    val df = new readData(conn).readCsv(args)
    val dataAnalysis= new DataAnalysis(conn, df, args)
    val write = new writeData(df, args)
   // dataAnalysis.createAnalysis()
    write.writeAnalysisCSV(dataAnalysis.createAnalysis())

  }
  else if (args.length == 4 && args(0) =="json"){
    println("*****************************************este é json **********************************************************************************")
    val df = new readData(conn).readJson(args)
    val dataAnalysis= new DataAnalysis(conn, df, args)
    val write = new writeData(df, args)
    write.writeAnalysisJson(dataAnalysis.createAnalysis())
  }
  else if (args.length == 8 && args(0) =="mysql"){
    println("*****************************************este é mysql **********************************************************************************")
    val df = new readData(conn).readSQL(args)
    val dataAnalysis= new DataAnalysis(conn, df, args)
    val write = new writeData(df, args)
    write.writeAnalysisSQL(dataAnalysis.createAnalysis())
  }
  else if (args.length == 5 && args(0) =="hive"){
    println("*****************************************este é hive **********************************************************************************")
    val df = new readData(conn).readHive(args)
    val dataAnalysis= new DataAnalysis(conn, df, args)
    val write = new writeData(df, args)
    write.writeAnalysisHIVE(dataAnalysis.createAnalysis())
  }
  else {
    println("Wrong input")
    println("for csv or txt Files you need five parameters. Example <file format> <delimiter> <url server> <source path> <destination path>")
    println("for json Files you need four parameters. Example <file format> <url server> <source path> <destination path>")
    println("for SQL Database you need seven parameters. Example <file format> <server name> <db name> <table name> <user> <password> <destination server> <destination path>")
    println("for HIVE Database you need five parameters. Example <file format> <db name> <table name> <destination server> <destination path>")

  }

}



