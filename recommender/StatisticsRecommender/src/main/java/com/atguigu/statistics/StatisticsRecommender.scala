package com.atguigu.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Movie 数据集
 *
 * 260                                         电影ID，mid
 * Star Wars: Episode IV - A New Hope (1977)   电影名称，name
 * Princess Leia is captured and held hostage  详情描述，descri
 * 121 minutes                                 时长，timelong
 * September 21, 2004                          发行时间，issue
 * 1977                                        拍摄时间，shoot
 * English                                     语言，language
 * Action|Adventure|Sci-Fi                     类型，genres
 * Mark Hamill|Harrison Ford|Carrie Fisher     演员表，actors
 * George Lucas                                导演，directors
 *
 */
case class Movie(mid:Int, name:String, descri:String, timelong:String, issue:String, shoot:String, language:String, genres:String, actors:String, directors:String)

/**
 * Rating数据集
 *
 * 1,31,2.5,1260759144
 */
case class Rating(uid:Int, mid:Int, score:Double, timestamp:Int)

/**
 * Tag数据集
 *
 * 15,1955,dentist,1193435061
 */
case class Tag(uid:Int, mid:Int, tag:String, timestamp:Int)

/**
 *
 * @param uri MongoDB连接
 * @param db  MongoDB数据库
 */
case class MongoConfig(uri:String, db:String)

case class Recommendation(uid:Int, score:Double)

case class GenresRecommendation( genre: String, recs: Seq[Recommendation] )

object StatisticsRecommender {
    // 定义常量
    val MONGODB_MOVIE_COLLECTION = "Movie"
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_TAG_COLLECTION = "Tag"
    val RATE_MORE_MOVIES = "RateMoreMovies"
    val RATE_MORE_RECENT_MOVIES = "RateMoreRecentMovies"
    val AVERAGE_MOVIES = "AverageMovies"
    val GENRES_TOP_MOVIES = "GenresTopMovies"

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

    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    ratingDF.createOrReplaceTempView("ratings")

    // 1. 历史热门电影统计
    val rateMoreMoviesDF = spark.sql("select mid, count(mid) as count from ratings group by mid order by count desc")
    storeDFinMongoDB(rateMoreMoviesDF, RATE_MORE_MOVIES)

    // 2. 最近热门电影统计
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changeDate", (x:Int) => {simpleDateFormat.format(new Date(x*1000L)).toInt})
    val ratingOfYearMonth = spark.sql("select mid, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentMoviesDF = spark.sql("select mid, count(mid) as count, yearmonth from ratingOfMonth group by yearmonth, mid sort by yearmonth desc, count desc")
    storeDFinMongoDB(rateMoreRecentMoviesDF, RATE_MORE_RECENT_MOVIES)

    // 3. 电影平均得分统计
    val averageScoreDF = spark.sql("select mid, avg(score) as avg from ratings group by mid")
    storeDFinMongoDB(averageScoreDF, AVERAGE_MOVIES)

    // 4. 每个类别优质电影统计
    val movieWithScore = movieDF.join(averageScoreDF, "mid")

    val genres = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery"
      , "Romance", "Science", "Tv", "Thriller", "War", "Western")
    val genresRDD = spark.sparkContext.makeRDD(genres)
    val genresTopMovieDF = genresRDD.cartesian(movieWithScore.rdd).filter {
      case (genre, row) => row.getAs[String]("genres").toLowerCase().contains(genre.toLowerCase())
    }.map {
      case (genre, row) => (genre, (row.getAs[Int]("mid"), row.getAs[Double]("avg")))
    }.groupByKey()
      .map {
        case (genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1, item._2)))
      }.toDF()
    storeDFinMongoDB(genresTopMovieDF, GENRES_TOP_MOVIES)

    spark.stop()
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
