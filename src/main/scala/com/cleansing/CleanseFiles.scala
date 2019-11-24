package com.cleansing

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

trait CleanseFiles {
  /* VATUCC Validation*/

  //V : Validity check of the data. e.g. card number contains only numbers,

  //A : Accuracy check, e.g. revenue has to have value rounded to 3 decimal places

  //T : Timeliness check of data e.g. if header date is sysdate-1

  //U : Uniqueness check, e.g. identify duplicate data

  //C : Consistency check, e.g. Child table primary keys are present in parent table

  //C : Completeness check e.g.

  def cleanseFile(spark : SparkSession, envProp : Config, file : DataFrame) : DataFrame = {
    //orderId, orderDate, orderCustomerId, orderStatus
    //orderId and orderCustomerId should not be null
    //no duplicate orderId
    val columns = file.select()
  }

}
