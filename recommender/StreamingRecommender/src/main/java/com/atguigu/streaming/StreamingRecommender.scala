package com.atguigu.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, StreamingContext, kafka010}
import redis.clients.jedis.Jedis

case class MongoConfig(uri:String, db:String)

case class Rating(uid:Int, mid:Int, score:Double, timestamp:Int)

case class Recommendation(mid:Int, score:Double)

case class UserRecs( uid: Int, recs: Seq[Recommendation] )

case class MovieRecs( mid: Int, recs: Seq[Recommendation] )

object ConnHelper{
  lazy val jedis = new Jedis("recommender")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://recommender:27017/recommender"))
}

object StreamingRecommender {
  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"


  /**
   * 模拟kafka生产出新的评分数据，此程序消费到新的评分数据后和redis最近的评分进行整合
   * 后对mongo数据库存放的推荐的相似电影进行比对，产生新的推荐电影数据存入mongo中
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://recommender:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    // 创建一个sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")
    // 创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, new Duration(2))
    import spark.implicits._

    // 将数据保存到MongoDB
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 取一部电影的 相似电影及相似度 并广播出去
    val simMovieMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map{
        recs => (recs.mid, recs.recs.map(x=>(x.mid, x.score)).toMap)
      }.collectAsMap()

    // 广播
    val simMovieMatrixBroadCast = sc.broadcast(simMovieMatrix)

    // kafka配置信息
    val kafkaPara = Map(
      "bootstrap.servers" -> "recommender:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    // 创建kafka消费流
    val kafkaStream = KafkaUtils.createDirectStream[String, String]( ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String]( Array(config("kafka.topic")), kafkaPara )
    )

    // 将消费到的UID|MID|SCORE|TIMESTAMP 转换成评分流
    val ratingStream = kafkaStream.map {
      msg =>
        val attr = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }

    ratingStream.foreachRDD{
      rdds => rdds.foreach{
        case(uid, mid, score, timestamp) => {
          println("rating data coming! >>>>>>>>>>>>>>>>")

          // 从redis中获取最新的电影评分
          val userRecentRatings = getUserRecentRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)

          // 获取最相似的候选电影，删去用户看过的电影
          val candidateMovies = getTopSimMovie(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)

          // 计算每个候选电影的评分
          val streamRecs = computeMovieScore(candidateMovies, userRecentRatings, simMovieMatrixBroadCast.value)

          // 存入mongoDB中
          saveIntoMongoDB(uid, streamRecs)
        }
      }
    }

    ssc.start()

    println(">>>>>>>>>>>>>>> streaming started!")

    ssc.awaitTermination()
  }

  // redis操作返回的是java类，为了用map操作需要引入转换类
  import scala.collection.JavaConversions._
  def getUserRecentRating(num: Int, uid: Int, jedis: Jedis) = {
    // 从redis读取数据，用户评分数据保存在 uid:UID 为key的队列里，value是 MID:SCORE
    jedis.lrange("uid:"+uid, 0, num)
      .map{
        item => {
          val attr = item.split("\\:")
          (attr(0).trim.toInt, attr(1).trim.toDouble)
        }
      }.toArray
  }

  def getTopSimMovie(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                    (implicit mongoConfig: MongoConfig)= {
    val allSimMovies = simMovies(mid).toArray

    //获取已经评分过的电影
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid" -> uid))
      .toArray
      .map(item => item.get("mid").toString.toInt)

    //过滤掉这些电影
    allSimMovies.filter(x => !ratingExist.contains(x._1))
      .sortWith(_._2>_._2)
      .take(num)
      .map(_._1)
  }

  def computeMovieScore(candidateMovies: Array[Int], userRecentRatings: Array[(Int, Double)],
                        simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]) = {
    // 定义一个ArrayBuffer，用于保存每一个备选电影的基础得分
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    // 定义一个HashMap，保存每一个备选电影的增强减弱因子
    val incount = scala.collection.mutable.HashMap[Int, Int]()
    val recount = scala.collection.mutable.HashMap[Int, Int]()

    for (candidateMovie <- candidateMovies; userRecentRating <- userRecentRatings) {
      val simScore = computeSimScore(candidateMovie, userRecentRating._1, simMovies)
      if (simScore > 0.7) {
        scores.append((candidateMovie, simScore * userRecentRating._2))
      }
      if(userRecentRating._2 > 3){
        incount(candidateMovie) = incount.getOrElse(candidateMovie, 0) + 1
      } else{
        recount(candidateMovie) = recount.getOrElse(candidateMovie, 0) + 1
      }
    }

    // 根据备选电影的mid做groupby，根据公式去求最后的推荐评分
    scores.groupBy(_._1)
      .map{
        case(candidateMovie, scoreList) =>
          (candidateMovie, (scoreList.map(_._2).sum) / scoreList.length + log(incount.getOrElse(candidateMovie, 1)) + log(recount.getOrElse(candidateMovie, 1)))
      }
  }

  // 获取两个电影之间的相似度
  def computeSimScore(mid1: Int, mid2: Int,
                      simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]):Double = {
    simMovies.get(mid1) match {
      case Some(sims) => sims.get(mid2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  // 求一个数的对数，利用换底公式，底数默认为10
  def log(m: Int):Double = {
    Math.log(m) / Math.log(10)
  }

  def saveIntoMongoDB(uid: Int, streamRecs: Map[Int, Double])(implicit mongoConfig: MongoConfig)={
    // 定义到StreamRecs表的连接
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    // 如果表中已有uid对应的数据，则删除
    streamRecsCollection.findAndRemove( MongoDBObject("uid" -> uid) )
    // 将streamRecs数据存入表中
    streamRecsCollection.insert(MongoDBObject("uid" -> uid,
        "recs" -> (streamRecs.map(item => MongoDBObject("mid" -> item._1, "score" -> item._2)))))
  }
}
