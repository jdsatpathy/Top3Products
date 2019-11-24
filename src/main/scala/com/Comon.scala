package com

import com.typesafe.config.Config
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Comon {
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
