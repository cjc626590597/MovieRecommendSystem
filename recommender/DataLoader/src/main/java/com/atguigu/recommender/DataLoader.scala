package com.atguigu.recommender

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

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

/**
 *
 * @param httpHosts       http主机列表，逗号分隔
 * @param transportHosts  transport主机端口
 * @param index            需要操作的索引
 * @param clusterName      集群名称
 */
case class ElasticConfig(httpHosts:String, transportHosts:String, index:String, clusterName:String)

object DataLoaderStudy {
    // 定义常量
    val MOVIE_DATA_PATH = "D:\\IdeaProjects\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
    val RATING_DATA_PATH = "D:\\IdeaProjects\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
    val TAG_DATA_PATH = "D:\\IdeaProjects\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

    val MONGODB_MOVIE_COLLECTION = "Movie"
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_TAG_COLLECTION = "Tag"
    val ES_INDEX = "Movie"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://recommender:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "recommender:9200",
      "es.transportHosts" -> "recommender:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "es-cluster"
    )

    // 创建一个sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    // 创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // 加载数据
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    val movieDF = movieRDD.map(
      item => {
        val fields = item.split("\\^")
        Movie(fields(0).toInt, fields(1).trim, fields(2).trim, fields(3).trim, fields(4).trim, fields(5).trim, fields(6).trim, fields(7).trim, fields(8).trim, fields(9).trim)
      }
    ).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(
      item => {
        val fields = item.split(",")
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble, fields(3).toInt)
      }
    ).toDF()

    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
    val tagDF = tagRDD.map(
      item => {
        val fields = item.split(",")
        Tag(fields(0).toInt, fields(1).toInt, fields(2).trim, fields(3).toInt)
      }
    ).toDF()

    // 将数据保存到MongoDB
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    storeDataInMongoDB(movieDF, ratingDF, tagDF)

    import org.apache.spark.sql.functions._

    // 数据预处理，把movie对应的tag信息添加进去，加一列 tag1|tag2|tag3...
    val newtag = tagDF.groupBy($"mid").agg(
      concat_ws("|", collect_set($"tag")).as("tags")
    ).select("mid", "tags")

    // newTag和movie做join，数据合并在一起，左外连接
    val movieDFwithTags = movieDF.join(newtag, Seq("mid"), "left")

    // 保存数据到ES
    implicit val esConfig = ElasticConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))
    storeDataInES(movieDFwithTags)(esConfig)
  }

  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    // 新建一个mongodb的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    // 如果mongodb中已经有相应的数据库，先删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    // 将DF数据写入对应的mongodb表中
    movieDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    mongoClient.close()
  }

  def storeDataInES(movieDF: DataFrame)(implicit eSConfig: ElasticConfig): Unit ={
    // 新建es配置
    val settings = Settings.builder().put("cluster.name", eSConfig.clusterName).build()

    // 新建一个es客户端
    val esClient = new PreBuiltTransportClient(settings)

    // 配置每个节点的主机名和端口
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    eSConfig.transportHosts.split(",")foreach {
      case REGEX_HOST_PORT(host, port) => {
        println((host, port))
        esClient.addTransportAddress(new InetSocketTransportAddress( InetAddress.getByName(host), port.toInt))
      }
    }

    // 先清理遗留的数据
    if (esClient.admin().indices().exists(new IndicesExistsRequest(eSConfig.index))
    .actionGet().isExists){
      esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
    }

    // 创建索引
    esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))

    // 将DF数据写入对应的索引中
    movieDF.write
      .option("es.nodes", eSConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index + "/" + ES_INDEX)
  }
}
