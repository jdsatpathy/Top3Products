package com.ingestion

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object IngestFiles {
  def main(args: Array[String]): Unit = {
    val envProp = ConfigFactory.load().getConfig(args(0))
    val sparkConf = sparkConfCreator(envProp)
    val spark = sparkSession(envProp, sparkConf)
    if (validateOrdersFile()) {
      ingestOrdersFile()
    }

    if (validateProductsFile()) {
      ingestProductsFile()
    }

    if (validateOrderItems()) {
      ingestOrderItems()
    }
  }

  def sparkConfCreator(envProp : Config) : SparkConf = {
    new SparkConf()
      .setAppName(envProp.getString("app.name"))
      .setMaster(envProp.getString("app.master"))
  }

  def sparkSession(envProp: Config, sparkConf: SparkConf) : SparkSession = {
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
  }

}
