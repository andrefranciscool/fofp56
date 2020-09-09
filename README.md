# Data Quality Automation Script

This script was developed under Scala and Spark with the intention of making an automated data quality analysis on several data sources, namely, csv files, json files, databases, etc. The output is stored in HDFS.

## Features

### General analysis with all attributes.

* Nulls Count
* Blank Spaces
* Not Nulls
* Maximum Length
* Minimun Length
* Distincts Count
* Maximal Value
* Minimal Value
* Rows Count

### Individual analysis of each attribute

* Value Count
* Frequency

## Help - Reading Options

### CSV Files

```
spark-submit --master yarn <jar file generated from sbt> <file format> <delimiter> <source path> <url server> <destination path>
```
### JSON Files

```
spark-submit --master yarn <jar file generated from sbt> <file format> <source path> <url server> <destination path>
```
### MySQL Database

```
spark-submit --master yarn <jar file generated from sbt> <file format> <MySQL server name> <db name> <table name> <user> <password> <url server> <destination path>
```
### Hive

```
spark-submit --master yarn <jar file generated from sbt> <file format> <db name> <table name> <url server> <destination path>
```

## Built With

* [IntelliJ IDEA](https://www.jetbrains.com/idea/) - IDE.
* [Hortonworks Data Platform](https://www.cloudera.com/downloads/hortonworks-sandbox/hdp.html) - Hortonworks Data Platform (HDP) Sandbox was configured using the cloud service of Microsoft Azure.

## Authors

* **André Leite** - [Ana Rita Dias](https://github.com/andrefranciscool)
* **Inês Machado** - [André Domingues](https://github.com/inesmachado98)
* **Maria Cardoso** - [Maria Cardoso](https://github.com/MariaCardoso97)
* **Rui Faria** - [Rui Faria](https://github.com/rmrfaria)

See also the list of [contributors](https://github.com/andrefranciscool/fofp56/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
