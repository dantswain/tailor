package org.tailor

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object Tailor {
  private val textSubtypes = Seq("me_message")
  def isTextSubtype(col: Column): Column =
    col.isin(textSubtypes: _*) || col.isNull

  def getEventsDf(spark: SparkSession, dataPath: String): DataFrame = {
    spark.read.parquet(dataPath + "events.parquet")
  }

  def getUsersDf(spark: SparkSession, dataPath: String): DataFrame = {
    spark.read.parquet(dataPath + "users.parquet")
  }

  def findCodeSnippets(df: DataFrame): DataFrame = {
    df.filter(isTextSubtype(col("subtype")))
      .select(col("text"))
      // reject a number of snippets that are just <http....>
      .filter(!col("text").rlike("<http.*>"))
      // only take columns with the fenceposts
      .filter(col("text").contains("```"))
      // extract what's between the fenceposts (first set)
      .withColumn("code", regexp_extract(col("text"), "(?s)```(.*?)```", 1))
      // slack appears to escape < and > all the time
      .withColumn("code", regexp_replace(col("code"), "&lt;", "<"))
      .withColumn("code", regexp_replace(col("code"), "&gt;", ">"))
  }

  def messagesByChannelDf(df: DataFrame): DataFrame = {
    df.filter(isTextSubtype(col("subtype")))
      .groupBy("channel")
      .count
      .select("channel", "count")
      .orderBy(col("count").desc)
  }

  def messagesByUserDf(df: DataFrame, usersDf: DataFrame): DataFrame = {
    df.filter(isTextSubtype(col("subtype")))
      .groupBy("user")
      .count
      .join(usersDf, col("user") === col("id"))
      .select("user", "name", "count")
      .orderBy(col("count").desc)
  }

  def messagesByHourDf(df: DataFrame): DataFrame = {
    df.select(hour(from_unixtime(col("ts"))).as("hour"))
      .groupBy(col("hour"))
      .count
      .orderBy(col("hour"))
  }
}
