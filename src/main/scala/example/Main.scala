package example

object Main extends App{
  val conn = new Connections()


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
  if (args.length == 0) {
    println("dude, i need at least one parameter")

  }else if (args.length == 5 && (args(0)=="csv" || args(0) == "txt")) {
    println("***************************************** CSV FILE **********************************************************************************")
    val df = new readData(conn).readCsv(args)
    val dataAnalysis= new DataAnalysis(conn, df, args)
    val write = new writeData(df, args)
   // dataAnalysis.createAnalysis()
    write.writeAnalysisCSV(dataAnalysis.createAnalysis())

  }
  else if (args.length == 4 && args(0) =="json"){
    println("***************************************** JSON FILE **********************************************************************************")
    val df = new readData(conn).readJson(args)
    val dataAnalysis= new DataAnalysis(conn, df, args)
    val write = new writeData(df, args)
    write.writeAnalysisJson(dataAnalysis.createAnalysis())
  }
  else if (args.length == 8 && args(0) =="mysql"){
    println("***************************************** SQL DATABASE ********************************************************************************")
    val df = new readData(conn).readSQL(args)
    val dataAnalysis= new DataAnalysis(conn, df, args)
    val write = new writeData(df, args)
    write.writeAnalysisSQL(dataAnalysis.createAnalysis())
  }
  else if (args.length == 5 && args(0) =="hive"){
    println("***************************************** HIVE DATABASE *******************************************************************************")
    val df = new readData(conn).readHive(args)
    val dataAnalysis= new DataAnalysis(conn, df, args)
    val write = new writeData(df, args)
    write.writeAnalysisHIVE(dataAnalysis.createAnalysis())
  }
  else {
    println("Wrong input")
    println("for CSV or txt Files you need five parameters. Example <file format> <delimiter>  <source path> <url server> <destination path>")
    println("for JSON Files you need four parameters. Example <file format> <source path> <url server> <destination path>")
    println("for SQL Database you need seven parameters. Example <file format> <server name> <db name> <table name> <user> <password> <url server> <destination path>")
    println("for HIVE Database you need five parameters. Example <file format> <db name> <table name> <url server> <destination path>")

  }

}



