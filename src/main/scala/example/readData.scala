package example

import org.apache.spark.sql.{DataFrame}

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

//Exemplo CSV
//spark-submit --master yarn fofp56_2.11-0.1.jar "csv" "," "/user/maria_dev/landing_zone/shootings.csv" "hdfs://20.56.49.233:8020/" "/user/maria_dev/spark_warehouse/generatedAnalysis/testeCSV/"

//Exemplo JSON
// spark-submit --master yarn fofp56_2.11-0.1.jar "json" "/user/maria_dev/landing_zone/generated.json" "hdfs://20.56.49.233:8020/" "/user/maria_dev/spark_warehouse/generatedAnalysis/json/"

//Exemplo SQL
//spark-submit --master yarn --jars /usr/share/java/mysql-connector-java.jar fofp56_2.11-0.1.jar "mysql" "remotemysql.com:3306" "O6FhOmVsS2" " shootings" "O6FhOmVsS2" "EEW1ugQnav"  "hdfs://20.56.49.233:8020/" "/user/maria_dev/spark_warehouse/generatedAnalysis/testemysql/"

//Exemplo HIVE
//spark-submit --master yarn fofp56_2.11-0.1.jar "hive" "foodmart" "product" "hdfs://20.56.49.233:8020/" "/user/maria_dev/spark_warehouse/generatedAnalysis/testeHive/"