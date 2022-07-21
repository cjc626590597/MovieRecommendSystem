package com.atguigu.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

/**
 * Rating数据集
 *
 * 1,31,2.5,1260759144
 */
case class MovieRating(uid:Int, mid:Int, score:Double, timestamp:Int)

case class MongoConfig(uri:String, db:String)

case class Recommendation(mid:Int, score:Double)

case class UserRecs( uid: Int, recs: Seq[Recommendation] )

case class MovieRecs( mid: Int, recs: Seq[Recommendation] )

object OfflineRecommender {
  // 定义常量
  val MONGODB_RATING_COLLECTION = "Rating"
  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"

  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://recommender:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    // 创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // 将数据保存到MongoDB
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => {(rating.uid, rating.mid, rating.score)})
      .cache()

    val userRDD = ratingRDD.map(_._1).distinct()
    val movieRDD = ratingRDD.map(_._2).distinct()

    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    val (rank, iterations, lambda) = (300, 5, 0.1)
    val model = ALS.train(trainData, rank, iterations, lambda)

    val userMovies = userRDD.cartesian(movieRDD)
    val preRatings = model.predict(userMovies)
    val useRecsDF = preRatings.map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (user, recs) => MovieRecs(user, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }.toDF()
    useRecsDF.show
    storeDFinMongoDB(useRecsDF, USER_RECS)

    val movieFeatures = model.productFeatures.map{
      case (mid, features) => (mid, new DoubleMatrix(features))
    }
    val movieCartesianRDD = movieFeatures.cartesian(movieFeatures)
    val movieRecsDF = movieCartesianRDD.filter {
      case (m1, m2) => m1._1 != m2._1
    }.map {
      case (m1, m2) => {
        (m1._1, (m2._1, this.consin(m1._2, m2._2)))
      }
    }.filter {
      _._2._2 > 0.6
    }.groupByKey()
      .map {
        case (mid, recs) => MovieRecs(mid, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }.toDF()
    storeDFinMongoDB(movieRecsDF, MOVIE_RECS)

    spark.stop()
  }

  def consin(features1:DoubleMatrix, features2:DoubleMatrix):Double ={
    features1.dot(features2) / (features1.norm2()*features2.norm2())
  }

  def storeDFinMongoDB(df:DataFrame, collection:String)(implicit mongoConfig: MongoConfig): Unit ={
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

}
