package example

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{ col, concat, countDistinct, desc, first, length, lit, max, min, round, sum, trim, when}
import org.apache.spark.sql.types.StructType

class DataAnalysis(conn: Connections, df: DataFrame, args: Array[String]) {
  val rowsCount = df.count()


  //TRIM
  def trimDf():DataFrame = {
    val df1 = df.select(df.columns.map(colName => {trim(col(colName).cast("String")) as s"${colName}"}): _*)
   // df1.show()
    return df1
  }

  //Nulls Values
  def getNulls():DataFrame = {
    val df1 = df.select(df.columns.map(colName => {sum(when(col(colName).isNull, 1).otherwise(0))as s"${colName}"}): _*)
    val df2 = df1.withColumn("Data Quality Report",lit("Nulls"))
   // df2.show()

    return df2
    //teste
}

  //Blanks Spaces Values
  def getBlanks():DataFrame = {
    val df1 = trimDf()
    val df2 = df1.select(df1.columns.map(colName => {sum(when(col(colName) === "", 1).otherwise(0))as s"${colName}"}): _*)
    val df3 = df2.withColumn("Data Quality Report",lit("Blank Spaces"))
   // df3.show()

    return df3
  }

  //Not Nulls Values Counting
  def getNotNulls():DataFrame = {
    val df1 = df.select(df.columns.map(colName => {sum(when(col(colName).isNotNull, 1).otherwise(0))as s"${colName}"}): _*)
    val df2 = df1.withColumn("Data Quality Report",lit("Not Nulls"))
  //  df2.show()

    return df2
  }

  //% filled
  def getFilled():DataFrame = {
    val df1 = trimDf().select(trimDf().columns.map(colName => {when(col(colName) === "", null).otherwise(col(colName)) as s"${colName}"}): _*)
    val df2 = df1.select(df1.columns.map(colName => {sum(when(col(colName).isNotNull, 1)) as s"${colName}"}): _*)
    val df3 = df2.select(df2.columns.map(colName => {(col(colName)/rowsCount)*100 as s"${colName}"}): _*)
    val df4 = df3.withColumn("Data Quality Report",lit("% Filled"))
 //   df4.show()

    return df4
  }

  def getDataTypes():DataFrame = {
    val types = df.schema.fields.map(f => f.dataType.toString)
    val df1 = df.select(df.columns.map(colName => {col(colName).cast("String") as s"${colName}"}): _*)

    val dataTypes = Row.fromSeq(types)
    val seqDataTypes = Seq(dataTypes)

    val df2 = conn.spark.createDataFrame(conn.spark.sparkContext.parallelize(seqDataTypes), StructType(df1.schema))

    val df3 = df2.withColumn("Data Quality Report",lit("Data Types"))
 //   df3.show()

    return df3
  }

  //Max Length
  def getMaxL():DataFrame = {
    val df1 = df.select(df.columns.map(c => max(length(col(c).cast("String"))).as(s"${c}")): _*)
    val df2 = df1.withColumn("Data Quality Report",lit("Maximum Length"))
  //  df2.show()

    return df2
  }

  //Min lenght
  def getMinL():DataFrame = {
    val df1 = df.select(df.columns.map(c => min(length(col(c).cast("String"))).as(s"${c}")): _*)
    val df2 = df1.withColumn("Data Quality Report",lit("Minimum Length"))
  //  df2.show()

    return df2
  }

  def transformBlankstoNulls():DataFrame = {
    val df1 = trimDf().select(trimDf().columns.map(colName => {when(col(colName) !== "", col(colName)).otherwise(null) as s"${colName}"}): _*)
  //  df1.show()

    return df1
  }

  //Maximal
  def getMax():DataFrame = {
    val df = transformBlankstoNulls()
    val df1 = df.select(df.columns.map(c => max(col(c).cast("String")).cast("String").as(s"${c}")): _*)
    val df2 = df1.withColumn("Data Quality Report",lit("Maximal"))
  //  df2.show()

    return df2
  }

  //Minimal
  def getMin():DataFrame = {
    val df = transformBlankstoNulls()
    val df1 = df.select(df.columns.map(c => min(col(c).cast("String")).as(s"${c}")): _*)
    val df2 = df1.withColumn("Data Quality Report",lit("Minimal"))
  //  df2.show()

    return df2
  }

  def getDistincts():DataFrame = {
    val df1 = df.select(df.columns.map(c => (countDistinct(col(c))).as(s"${c}")): _*)
    val df2 = df1.withColumn("Data Quality Report",lit("Distincts Count"))
   // df2.show()

    return df2
  }

  def getRowCount():DataFrame = {
    //Rows Count
    val emptyDF = conn.spark.createDataFrame(conn.spark.sparkContext.emptyRDD[Row], df.schema)
    val emptyDF2 = emptyDF.select(df.columns.map(colName => {sum(when(col(colName).cast("String") === "", "-")).cast("String") as s"${colName}"}): _*)
    val df1 = emptyDF2.withColumn("Data Quality Report", concat(lit("Rows Count: "), lit(rowsCount)))
    // df1.show()
    return df1
  }

  def createAnalysis():DataFrame = {
    val dfs = Seq(getNulls(),getBlanks(), getNotNulls(), getMaxL(), getMinL(),getDistincts(), getMax(),getMin(), getRowCount())//adicionar ainda o df71
    val dfs2 = dfs.reduce(_ unionAll _)
    dfs2.show()
    perAttribute()
    return dfs2
  }


  def perAttribute() = {
    for( a <- 0 to (df.columns.size-1)) {
      val dfs = df.groupBy(df.columns(a)).count().as("count")
      val dfs2 = dfs.withColumn("% Frequency", round((dfs.col("Count") / rowsCount) * 100, 2))
      val dfs3 = dfs2.orderBy(desc("% Frequency")).limit(20)
     // dfs3.show()


      val nameFile = dfs2.schema.fields.map(f => f.name)
      val nameFile2 = nameFile(0)
      val pathResult = args.length - 1
      val server = args.length - 2
      val dfs4 = dfs3.withColumn(nameFile2, dfs3.col(nameFile2).cast("String"))

      try{
        dfs4.coalesce(1).write.mode("append")
          .option("sep", ";")
          .option("header", "true")
          .option("encoding", "ISO-8859-1")
          .csv(args(server)+ args(pathResult) + "attributes/" + nameFile2 )
      }
      catch {
        case x : Exception => println("Destination Path already exists. Please choose another one")
      }
    }
  }


}
