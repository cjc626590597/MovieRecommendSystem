package com.atguigu.content

import org.apache.spark.ml.linalg.{SparseMatrix, SparseVector}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

case class Movie(mid:Int, name:String, descri:String, timelong:String, issue:String, shoot:String, language:String, genres:String, actors:String, directors:String)

case class MongoConfig(uri:String, db:String)

case class Recommendation(mid:Int, score:Double)

case class MovieRecs( mid: Int, recs: Seq[Recommendation] )

object ContentRecommender {
  // 定义常量
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val CONTENT_MOVIE_RECS = "ContentMovieRecs"

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

    val movieDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .map(movie => (movie.mid, movie.name, movie.genres.map(c => if(c=='|') ' ' else c)))
      .toDF("mid", "name", "genres")
      .cache()

    // genres分词
    val tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")
    val wordsData = tokenizer.transform(movieDF)

    // 将分词转化为对应词频
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("features").setNumFeatures(50)
    val features = hashingTF.transform(wordsData)

    // 利用IDF模型将词频转为逆文档频率
    val iDFModel = new IDF().setInputCol("features").setOutputCol("featuresNums")
    val iDFModel1 = iDFModel.fit(features)
    val featuresNums = iDFModel1.transform(features)

    val featuresRDD = featuresNums.map(
      row => (row.getAs[Int]("mid"), row.getAs[SparseVector]("featuresNums").toArray)
    )
      .rdd
      .map(
        row => (row._1, new DoubleMatrix(row._2))
      )
    val movieCartesianRDD = featuresRDD.cartesian(featuresRDD)
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
    storeDFinMongoDB(movieRecsDF, CONTENT_MOVIE_RECS)

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
