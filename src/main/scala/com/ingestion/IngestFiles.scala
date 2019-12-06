package com.ingestion

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cleansing.CleanseFiles
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import com.validation.{EnvPropertiesValidator, ValidateFiles}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object IngestFiles extends LazyLogging with CleanseFiles with ValidateFiles with EnvPropertiesValidator with Serializable {
  def main(args: Array[String]): Unit = {
    val envProp = ConfigFactory.load().getConfig(args(0))
    val sparkConf = sparkConfCreator(envProp)
    val spark = sparkSessionWithHive(envProp, sparkConf)
    val fileName = envProp.getString("file.name")
    val filePath = envProp.getString("input.path")
    //    try {
    val readFile = readFileToDataFrame(spark, filePath, envProp)
    val timestamp1 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now())
    saveAsJson(spark,envProp,readFile, "readFile", timestamp1)
    logger.error(s"File read count : ${readFile.count()}")
    logger.error(s"File read schema : ${readFile.printSchema()}")
    val cleansedFile = cleanseFile(spark, envProp, readFile)
    logger.error(s"Cleansed File count : ${cleansedFile.count()}")
    cleansedFile.persist(StorageLevel.MEMORY_ONLY)
    val timestamp2 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now())
    saveAsJson(spark, envProp, cleansedFile, "cleansedFile", timestamp2)
    if (insertToStagingTable(spark, cleansedFile, envProp)) {
      logger.error(s"$fileName file loaded successfully to staging table")
    }
    else {
      logger.error(s"$fileName file loaded partially to staging table")
      throw new Exception("Partial load exception")
    }
    //    } catch {
    //      case e: Exception => logger.error(s"Exception occurred : ${e.getCause.getStackTrace}");
    //        throw new Exception("Job failed")
    //    }
  cleansedFile.unpersist()
  }

  def sparkConfCreator(envProp: Config): SparkConf = {
    new SparkConf()
      .setAppName(envProp.getString("app.name"))
      .setMaster(envProp.getString("app.master"))
  }

  def sparkSessionDev(envProp: Config, sparkConf: SparkConf): SparkSession = {
    SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
  }

  def sparkSessionWithHive(envProp: Config, sparkConf: SparkConf): SparkSession = {
    SparkSession
      .builder()
      .config("hive.metastore.uris", envProp.getString("hive.metastore"))
      .config("spark.sql.warehouse.dir", envProp.getString("warehouse.dir"))
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
  }

  def sparkSessionSelect(sparkConf: SparkConf, envProp: Config, arg: String): SparkSession = arg match {
    case "dev" => sparkSessionDev(envProp, sparkConf)
    case "prod" => sparkSessionWithHive(envProp, sparkConf)
    case _ => sparkSessionDev(envProp, sparkConf)
  }

  def insertToStagingTable(spark: SparkSession, df: DataFrame, envProp: Config): Boolean = {
    val preLoadCount = df.count()
    df.write.mode("overwrite").insertInto(envProp.getString("table.name"))
    val postLoadCount = spark.read.table(envProp.getString("table.name")).count()
    if (preLoadCount == postLoadCount) {
      logger.error(s"Staging table loaded with $postLoadCount records")
      true
    } else {
      logger.error(s"Data read count : $preLoadCount \n Data load count : $postLoadCount \n Counts does not match..!!")
      false
    }
  }

  def readFileToDataFrame(spark: SparkSession, filePath: String, envProp: Config): DataFrame = {
    val schemaFilePath = envProp.getString("schema.file.path")
    val field: Array[StructField] = scala.io.Source.fromFile(schemaFilePath).getLines().toArray.map(m =>
      m.split(":")(1) match {
        case "int" => StructField(m.split(":")(0), org.apache.spark.sql.types.IntegerType, true)
        case _ => StructField(m.split(":")(0), org.apache.spark.sql.types.StringType, true)
      }
    )
    val schema = StructType(field)
    spark.read.
      option("mode", envProp.getString("read.mode")).
      option("header", envProp.getBoolean("header.exists")).
      schema(schema).
      format(envProp.getString("file.format")).
      load(filePath)
  }

  def saveAsJson(spark: SparkSession, envProp : Config, df : DataFrame, filename : String, timestamp : String) : Unit = {
    logger.error(s"Starting to read ${filename}")
    val countReceived = df.count()
    df.foreach(x => logger.error(x.toString()))
    df.write.json(envProp.getString("output.path") + timestamp)
    //val countWritten = spark.read.json(envProp.getString("output.path") + timestamp).count()
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(new Path(envProp.getString("output.path") + timestamp + "/_SUCCESS"))) {
      logger.error(s"File $filename written successfully as json with count : ${countReceived} in ${envProp.getString("output.path")}${timestamp}/")
    }
    else {
      logger.error(s"Received file $filename count ${countReceived} does not match with written file count : ${spark.read.json(envProp.getString("output.path") + timestamp).count()} in ${envProp.getString("output.path")}${timestamp}/")
    }
  }
}
