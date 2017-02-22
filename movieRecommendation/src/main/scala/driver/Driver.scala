package driver

import Recommendation.ALSRecommendation
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by aman.verma on 22/2/17.
  */

object Driver {
  private val SPARK_JOB_NAME = "movieRecommendation"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(SPARK_JOB_NAME).setMaster("local[2]").set("spark.executor.memory","1g");
    val sc = new SparkContext(sparkConf)
    ALSRecommendation.ALSRecommendationMovies(sc)
    sc.stop()
  }
}
