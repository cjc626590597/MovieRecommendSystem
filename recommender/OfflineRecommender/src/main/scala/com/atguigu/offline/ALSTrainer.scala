package com.atguigu.offline

import breeze.numerics.sqrt
import com.atguigu.offline.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://recommender:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ALSTrainer")
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
      .map(rating => {Rating(rating.uid, rating.mid, rating.score)})
      .cache()

     val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
     val trainData = splits(0)
     val testData = splits(1)
     adjustALSParam(trainData, testData)

    spark.stop()
  }

  def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {
    val results = for (rank <- Array(50, 100, 200, 300); lambda <- Array(0.01, 0.1, 1))
      yield {
        val model = ALS.train(trainData, rank, 5, lambda)
        val rmse = getRMSE(testData, model)
        (rank, lambda, rmse)
      }
      println(results.minBy(_._3))
  }

  def getRMSE(data: RDD[Rating], model: MatrixFactorizationModel):Double = {
    val userProduct = data.map(item => (item.user, item.product))
    val target = data.map(item => ((item.user, item.product), item.rating))
    val pred = model.predict(userProduct)
        .map{
          case rating => ((rating.user, rating.product), rating.rating)
        }
    sqrt(target.join(pred).map{
      case ((user, product), (target, pred)) =>
        val error = target - pred
        error * error
    }.mean())
  }

}
