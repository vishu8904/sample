package com.spg.spear

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.sql.Date

import org.apache.spark.sql.expressions.Window

object GetDerivedColumns {

  val spark = SparkSession.builder().master("local[*]").appName("GetDerivedColumns").getOrCreate()

  def getColumnVintage():Unit = {

  }

  def getColumnQuarter():Unit = {

  }

  def getColumnDetailedQuarter():Unit = {

  }

  def getColumnCPR():Unit = {

  }


  def EndDate(input:java.sql.Date): java.sql.Date = {
    var quarter:java.sql.Date = null
    val month = input.getMonth
    if (month >=0 && month<=2){
      quarter = new java.sql.Date(input.getYear,2, 31)
    }
    else if (month >= 3 && month <= 5){
      quarter = new java.sql.Date(input.getYear,5, 30)
    }
    else if (month >= 6 && month <= 8){
      quarter = new java.sql.Date(input.getYear,8, 30)
    }
    else {
      quarter = new java.sql.Date(input.getYear,11, 31)
    }

    quarter

  }


  def quarterDate (dealid:Long,nextdealid:Long,nextnexdealid:Long,Quarter:Int,nextQuarter:Int,Quarter1:Double,nextQuarter1:Double,nexnexQuarter1:Double,TDate:java.sql.Date): java.sql.Date = {
    var date :java.sql.Date = null
    if (Quarter1.toString.length == 1){
      date = EndDate(TDate)
    }
    else if (nextQuarter1.toString.length ==1 && dealid == nextdealid) {
      date = null

    }
    else if (nexnexQuarter1.toString.length==1 && dealid == nextnexdealid){
      date = null
    }
    else if (Quarter == nextQuarter && dealid == nextdealid){
      date = null
    }
    else{
      date = EndDate(TDate)

    }

    date

  }



  def main(args:Array[String]):Unit = {

    val initDF = spark.read.option("header","True").csv("C:\\IDF\\Master Deal Info files\\part-00000-cf53b862-23a5-47ce-98fe-a023f24a48f9-c000.csv")
    val initDF1 = initDF.withColumn("ClosingDate",to_date(unix_timestamp(col("ClosingDate"),"yyyy-MM-dd").cast("timestamp")))
    val initDFWithBasicColumns = initDF1.withColumn("TransactionDate",to_date(unix_timestamp(col("TransactionDate"),"yyyy-MM-dd").cast("timestamp")))
    val ageDF = initDFWithBasicColumns.withColumn("Age",months_between(col("TransactionDate"),col("ClosingDate")).cast("integer"))

    print(ageDF.show)

    val winSpec = Window.partitionBy(("DEAL_ID")).orderBy(asc("TransactionDate"))

    val quarterDateUDF = udf[Date,Long,Long,Long,Int,Int,Double,Double,Double,Date](quarterDate)

    val quarterDateDF = ageDF.withColumn("QuarterDate",
      quarterDateUDF(
        col("DEAL_ID"),
        lead(col("DEAL_ID"),1).over(winSpec),
      lead(col("DEAL_ID"),2).over(winSpec),
      col("Quarter"),
        lead(col("Quarter"),1).over(winSpec),
        col("DetailedQuarter"),
        lead(col("DetailedQuarter"),1).over(winSpec),
        lead(col("DetailedQuarter"),2).over(winSpec),
        col("TransactionDate")
      )
    )

      //print(quarterDateDF.show())

  }

}
