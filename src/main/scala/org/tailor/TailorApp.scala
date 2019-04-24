package org.tailor

import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object TailorApp {
  // change this for your slack!
  private val dataPath: String = "parquet_data/rocdev/"

  def main(args: Array[String]): Unit = {
    println("vvvv SPARK SETUP LOGGING")
    val spark = SparkSession.builder.appName("Tailor").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("^^^ END SPARK SETUP LOGGING")
    println("")

    val df = Tailor.getEventsDf(spark, dataPath).cache
    val usersDf = Tailor.getUsersDf(spark, dataPath)
    val byChannelDf = Tailor.messagesByChannelDf(df)
    val byUserDf = Tailor.messagesByUserDf(df, usersDf)
    val byHourDf = Tailor.messagesByHourDf(df)

    println(s"Event Count: ${df.count()}")
    println(s"User Count: ${usersDf.count()}")

    println("Top channels by message count:")
    byChannelDf.show

    println("Top users by message count:")
    byUserDf.show

    println("Activity by hour:")
    byHourDf.show(numRows = 24)

    if (Files.exists(Paths.get("models/"))) {
      runModel(spark, df, "models/")
    } else {
      println(
        "No models found - run the trainer and then rerun " +
          "this to see language analysis"
      )
    }
  }

  private def runModel(
      spark: SparkSession,
      df: DataFrame,
      modelPath: String
  ): Unit = {
    val model = CodeClassifier.load(spark, modelPath)

    val codeSnippetsDf = Tailor
      .findCodeSnippets(df)
      .withColumnRenamed("code", "value")
    val predictions =
      CodeClassifier.predict(model, codeSnippetsDf)
    predictions
      .select("language", "value")
      .show(truncate = false)

    predictions.groupBy("language").count().orderBy(col("count").desc).show()
    val json = predictions
      .select("language", "value")
      .toJSON
      .collect
      .mkString("[", ",", "]")

    Files.write(
      Paths.get("language_predictions.json"),
      json.getBytes(StandardCharsets.UTF_8)
    )

    spark.stop()
  }
}
