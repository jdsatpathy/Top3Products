package com.ingestion

import com.cleansing.CleanseFiles
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import com.validation.{EnvPropertiesValidator, ValidateFiles}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object IngestFiles extends LazyLogging with CleanseFiles with ValidateFiles with EnvPropertiesValidator{

  def main(args: Array[String]): Unit = {
    val envProp = ConfigFactory.load().getConfig(args(0))
    val sparkConf = sparkConfCreator(envProp)
    val spark = sparkSessionWithHive(envProp, sparkConf)
    val fileName = envProp.getString("file.name")
    val filePath = envProp.getString(envProp.getString("input.path"))
    try {
      val cleansedFile = cleanseFile(spark, envProp, readFileToDataFrame(spark,filePath,envProp))
      if (insertToStagingTable(spark, cleansedFile, envProp)) {
        logger.info(s"$fileName file loaded successfully to staging table")
      }
      else {
        logger.info(s"$fileName file loaded partially to staging table")
        throw new Exception("Partial load exception")
      }
    } catch {
      case e : Exception => logger.error(s"Exception occurred : ${e.getStackTrace}")
    }
  }

  def sparkConfCreator(envProp : Config) : SparkConf = {
    new SparkConf()
      .setAppName(envProp.getString("app.name"))
      .setMaster(envProp.getString("app.master"))
  }

  def sparkSessionDev(envProp: Config, sparkConf: SparkConf) : SparkSession = {
    SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
  }

  def sparkSessionWithHive(envProp: Config, sparkConf: SparkConf) : SparkSession = {
    SparkSession
      .builder()
      .config("hive.metastore.uris",envProp.getString("hive.metastore"))
      .config("spark.sql.warehouse.dir", envProp.getString("warehouse.dir"))
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
  }

  def sparkSessionSelect(sparkConf: SparkConf, envProp: Config, arg : String) : SparkSession = arg match {
    case "dev" => sparkSessionDev(envProp, sparkConf)
    case "prod" => sparkSessionWithHive(envProp,sparkConf)
    case _ => sparkSessionDev(envProp,sparkConf)
  }

  def insertToStagingTable(spark: SparkSession, df : DataFrame, envProp : Config) : Boolean = {
    val preLoadCount = df.count()
    df.write.mode("overwrite").insertInto(envProp.getString("table.name"))
    val postLoadCount = spark.read.table(envProp.getString("table.name")).count()
    if (preLoadCount == postLoadCount) {
      logger.info(s"Staging table loaded with $postLoadCount records")
      true
    } else {
      logger.error(s"Data read count : $preLoadCount \n Data load count : $postLoadCount \n Counts does not match..!!")
      false
    }
  }

  def readFileToDataFrame(spark : SparkSession, filePath : String, envProp : Config) : DataFrame = {
    val schemaFilePath = envProp.getString("schema.file.path")
    val schema = new StructType()
    spark.read.textFile(schemaFilePath).foreach(r => r.split(":")(0) match {
      case "int" => schema.add(StructField(r.split(":")(0),IntegerType, true))
      case _ => schema.add(StructField(r.split(":")(0),StringType, true))
    })
    spark.read
      .option("mode", envProp.getString("read.mode"))
      .option("header", envProp.getBoolean("header.exists"))
      .schema(schema)
      .format(envProp.getString("file.format"))
      .load(filePath)
  }
}
