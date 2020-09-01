package example

import example.Main.conn
import org.apache.spark.sql.DataFrame

private class readData(conn: Connections) {

  def readCsv(args: Array[String]): DataFrame = {
    val dataframe = conn.spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", args(1))
      .option("encoding", "ISO-8859-1")
      .csv(args(2) + args(3))
    return dataframe
  }

  def readJson(args: Array[String]): DataFrame = {
    val dataframe = conn.spark.read.option("multiline", "true").json(args(1)+args(2))
    //val dataframe = conn.spark.read.option("multiline", "true").json("C:\\Users\\andre\\Downloads\\generated.json")
    return dataframe
  }

}
