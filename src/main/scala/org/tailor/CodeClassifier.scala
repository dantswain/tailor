package org.tailor

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, IDFModel}
import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}

case class LangRow(label: Integer, language: String, languages: DataFrame)

case class CodeClassifierModel(
    idfModel: IDFModel,
    bayesModel: NaiveBayesModel,
    languages: DataFrame
)

object CodeClassifierTrainer {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder.appName("CodeClassifier.Trainer").getOrCreate()
    val model = CodeClassifier.fromTrainingData(spark, "training_data/")
    CodeClassifier.write(model, "models/")
  }
}

object CodeClassifier {
  private val tokenizer =
    new Tokenizer().setInputCol("value").setOutputCol("words")
  private val hashingTF =
    new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
  private val idf =
    new IDF().setInputCol("rawFeatures").setOutputCol("features")
  private val naiveBayes = new NaiveBayes()

  def write(model: CodeClassifierModel, path: String): Unit = {
    model.idfModel.write.save(s"$path/idf")
    model.bayesModel.write.save(s"$path/bayes")
    model.languages.write.parquet(s"$path/languages")
  }

  def load(spark: SparkSession, path: String): CodeClassifierModel = {
    new CodeClassifierModel(
      IDFModel.load(s"$path/idf"),
      NaiveBayesModel.load(s"$path/bayes"),
      spark.read.parquet(s"$path/languages")
    )
  }

  def fromTrainingData(
      spark: SparkSession,
      trainingPath: String
  ): CodeClassifierModel = {
    import spark.implicits._

    val languages = listTrainingLanguages(trainingPath)
    val languagesDf = languages.toDF("language", "label")

    val trainingData = loadTrainingData(spark, languages)
    val idfModel = buildIdfModel(trainingData)
    val bayesModel = buildModel(trainingData, idfModel)

    new CodeClassifierModel(idfModel, bayesModel, languagesDf)
  }

  def calcFeatures(model: CodeClassifierModel, df: DataFrame): DataFrame = {
    val featurized = hashingTF.transform(df)
    model.idfModel.transform(featurized)
  }

  def predict(
      model: CodeClassifierModel,
      df: DataFrame
  ): DataFrame = {
    val tokenized = tokenizer.transform(df)
    val featurized = calcFeatures(model, tokenized)
    model.bayesModel
      .transform(featurized)
      .withColumn("label", col("prediction").cast("Int"))
      .join(model.languages, Seq("label"))
  }

  private def loadLanguage(
      spark: SparkSession,
      label: Integer,
      lang: String
  ): DataFrame = {
    tokenizer.transform(
      spark.read.text(s"training_data/$lang/*").withColumn("label", lit(label))
    )
  }

  private def loadTrainingData(
      spark: SparkSession,
      languages: Seq[(String, Int)]
  ): DataFrame = {
    import spark.implicits._

    languages
      .map {
        case (lang, label) => loadLanguage(spark, label, lang)
      }
      .reduce(_ union _)
  }

  private def listTrainingLanguages(
      trainingPath: String
  ): Seq[(String, Int)] = {
    (new File(trainingPath)).listFiles
      .filter(_.isDirectory)
      .map(_.toPath.getFileName.toString)
      .toSeq
      .zipWithIndex
  }

  private def buildIdfModel(df: DataFrame): IDFModel = {
    val featurized = hashingTF.transform(df)
    idf.fit(featurized)
  }

  private def buildModel(df: DataFrame, idfModel: IDFModel): NaiveBayesModel = {
    val featurized = idfModel.transform(hashingTF.transform(df))
    naiveBayes.fit(featurized)
  }
}

// object CodeClassifier {
//   private val tokenizer =
//     new Tokenizer().setInputCol("value").setOutputCol("words")
//   val hashingTF =
//     new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
//   val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
//
//   val naiveBayes = new NaiveBayes()
//
//   def train(spark: SparkSession, trainingPath: String): CodeClassifierModel = {
//     val languages = listTrainingLanguages(trainingPath)
//   }
//
//   def listTrainingLanguages(trainingPath: String): Seq[(String, Int)] = {
//     (new File(trainingPath)).listFiles
//       .filter(_.isDirectory)
//       .map(_.toPath.getFileName.toString)
//       .toSeq
//       .zipWithIndex
//   }
//
//   def languagesDf(spark: SparkSession): DataFrame = {
//     import spark.implicits._
//
//     languages.toDF("label", "language")
//   }
//
//   def loadLanguage(
//       spark: SparkSession,
//       label: Integer,
//       lang: String
//   ): DataFrame = {
//     tokenizer.transform(
//       spark.read.text(s"training_data/$lang/*").withColumn("label", lit(label))
//     )
//   }
//
//   def loadTrainingData(
//       spark: SparkSession,
//       languages: Seq[(String, Int)]
//   ): DataFrame = {
//     import spark.implicits._
//
//     languages
//       .map {
//         case (lang, label) => loadLanguage(spark, label, lang)
//       }
//       .reduce(_ union _)
//   }
//
//   def buildIdfModel(df: DataFrame): IDFModel = {
//     val featurized = hashingTF.transform(df)
//     idf.fit(featurized)
//   }
//
//   def calcFeatures(df: DataFrame, idfModel: IDFModel): DataFrame = {
//     val featurized = hashingTF.transform(df)
//     idfModel.transform(featurized)
//   }
//
//   def buildModel(df: DataFrame, idfModel: IDFModel): NaiveBayesModel = {
//     val featurized = calcFeatures(df, idfModel)
//     naiveBayes.fit(featurized)
//   }
//
//   def predict(
//       spark: SparkSession,
//       df: DataFrame,
//       idfModel: IDFModel,
//       model: NaiveBayesModel
//   ): DataFrame = {
//     val tokenized = tokenizer.transform(df)
//     val featurized = calcFeatures(tokenized, idfModel)
//     model
//       .transform(featurized)
//       .withColumn("label", col("prediction").cast("Int"))
//       .join(languagesDf(spark), Seq("label"))
//   }
// }
