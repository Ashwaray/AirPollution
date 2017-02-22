package Recommendation

/**
  * Created by ashwaray on 22/02/17.
  */

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
object ALSRecommendation {
  case class Rating(userId: Long, movieId: Long, rating: Float, timestamp: Long)

  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    Rating(fields(0).trim.toLong, fields(1).trim.toLong, fields(2).trim.toFloat, fields(3).trim.toLong)
  }

  def ALSRecommendationMovies(sc: SparkContext): Unit = {
    val sqlContext = new SQLContext(sc)
    val ratings = sc.textFile("/Users/ashwaray/myWork/ml-latest-small/ratingSample.txt").map(line => parseRating(line))
    val ratingDF =  sqlContext.createDataFrame(ratings)
    val Array(training, test) = ratingDF.randomSplit(Array(0.8, 0.2))

    print("training size " + training.count())
    print("test size " + test.count())

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(training)
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")
    sc.stop()
  }
}

