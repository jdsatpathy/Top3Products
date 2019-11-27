package com.cleansing

import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
trait CleanseFiles {
  def cleanseFile(spark : SparkSession, envProp : Config, file : DataFrame) : DataFrame = {
    def discardNull(df : DataFrame, nonNullFields : Int*) : DataFrame = {
      if (nonNullFields == Nil || nonNullFields.isEmpty)
        df
      else {
        if (nonNullFields.tail != Nil) {
          discardNull(df.filter(col(df.columns(nonNullFields.head)).isNotNull), nonNullFields.tail : _*)
        } else {
          df.filter(col(df.columns(nonNullFields.head)).isNotNull)
        }
      }
    }

    def numberOnly(df: DataFrame, numberOnlyFields : Int*) : DataFrame = {
      def isAllDigits(str : String) : Boolean = str.forall(_.isDigit)
      if (numberOnlyFields.nonEmpty) {
        if(numberOnlyFields.tail != Nil)
          numberOnly(df.filter(m => isAllDigits(m(numberOnlyFields.head).toString)), numberOnlyFields.tail : _*)
        else
          df.filter(m => isAllDigits(m(numberOnlyFields.head).toString))
      } else
        df
    }


    val distinctData = file.filter(r => r.toString().split(",").length == envProp.getInt("record.length")).distinct() // Ensures no duplicate
    val nonNullCols : Seq[Int] = envProp.getString("non.null.columns").split(",").map(_.toInt) // Ensures no null passed
    val numberOnlyFields = envProp.getString("non.string.columns").split(",").map(_.toInt)
    val nonNullDf = discardNull(distinctData, nonNullCols : _*)
    numberOnly(nonNullDf, numberOnlyFields : _*)


//    def readAsText(file : String) : DataFrame = {
//      spark.read.textFile(file).filter(r => r.split(envProp.getString("file.delimiter")) == envProp.getInt("record.length"))
//    }
  }

}
