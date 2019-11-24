package com.ingestion


import com.cleansing.CleanseFiles
import com.typesafe.config.{ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import com.validation.{EnvPropertiesValidator, ValidateFiles, ValidateOrdersFile}

class IngestOrdersFile extends LazyLogging with CleanseFiles with ValidateFiles with EnvPropertiesValidator{








}
