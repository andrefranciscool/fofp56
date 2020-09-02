package example

import example.Main.conn
import org.apache.spark.sql.{DataFrame, SparkSession}

private class readData(conn: Connections) {


  def readCsv(args: Array[String]): DataFrame = {
    val dataframe = conn.spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", args(1))
      .option("encoding", "ISO-8859-1")
      .csv(args(3) + args(2))
    return dataframe
  }

  def readJson(args: Array[String]): DataFrame = {
    val dataframe = conn.spark.read.option("multiline", "true").json(args(2) + args(1))
    //val dataframe = conn.spark.read.option("multiline", "true").json("C:\\Users\\andre\\Downloads\\generated.json")
    return dataframe
  }

  def readSQL(args: Array[String]): DataFrame = {
    val dataframe = conn.spark.sqlContext.read.format("jdbc")
      .option("url", "jdbc:mysql://" + args(1) + "/" + args(2))
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", args(3))
      .option("user", args(4))
      .option("password", args(5))
      .load()
    return dataframe
  }

  def readHive(args: Array[String]): DataFrame = {
    val myDataFrame = conn.spark.sql("select * from " + args(1)+"."+args(2))
    return myDataFrame
  }
}