package com.cleansing

import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.annotation.tailrec

trait CleanseFiles {
  /* VATUCC Validation*/

  //V : Validity check of the data. e.g. card number contains only numbers,

  //A : Accuracy check, e.g. revenue has to have value rounded to 3 decimal places

  //T : Timeliness check of data e.g. if header date is sysdate-1

  //U : Uniqueness check, e.g. identify duplicate data

  //C : Consistency check, e.g. Child table primary keys are present in parent table

  //C : Completeness check e.g.

  def cleanseFile(spark : SparkSession, envProp : Config, file : DataFrame) : DataFrame = {
    /*orderId, orderDate, orderCustomerId, orderStatus
    Col 1 and 3 if null then discard -- done
    no duplicate rows --done
    1st and 3rd column should be numbers --done
    should have n columns in each row
    */
    def discardNull(df : DataFrame, nonNullFields : Int*) : DataFrame = {
      if (nonNullFields == Nil || nonNullFields.isEmpty)
        df
      else {
        if (nonNullFields.tail != Nil) {
          discardNull(df.filter(col(df.columns(nonNullFields.head)) =!= lit("null")), nonNullFields.tail : _*)
        } else {
          df.filter(col(df.columns(nonNullFields.head)) =!= lit("null"))
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
    val nonNullCols = envProp.getString("non.null.columns").split(",").map(_.toInt) // Ensures no null passed
    val numberOnlyFields = envProp.getString("non.string.columns").split(",").map(_.toInt)
    val nonNullDf = discardNull(distinctData, nonNullCols : _*)
    numberOnly(nonNullDf, numberOnlyFields : _*)


//    def readAsText(file : String) : DataFrame = {
//      spark.read.textFile(file).filter(r => r.split(envProp.getString("file.delimiter")) == envProp.getInt("record.length"))
//    }
  }

}
