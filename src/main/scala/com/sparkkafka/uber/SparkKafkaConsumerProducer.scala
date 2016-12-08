package com.sparkkafka.uber

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{ DStream, InputDStream }
import org.apache.spark.streaming.kafka.v09.KafkaUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.streaming.kafka.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.rdd.RDD

object SparkKafkaConsumerProducer extends Serializable {

  // schema for uber data   
  case class Uber(dt: String, lat: Double, lon: Double, base: String) extends Serializable

  val timeout = 10 // Terminate after N seconds
  val batchSeconds = 2 // Size of batch intervals

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      throw new IllegalArgumentException("You must specify the topics, for example  /user/user01/stream:ubers /user/user01/stream:uberp ")
    }

    //val topics: String = args(0)
    val Array(topics, topicp) = args
    System.out.println("Subscribed to : " + topics)

    val brokers = "maprdemo:9092" // not needed for MapR Streams, needed for Kafka
    val groupId = "sparkApplication"
    val batchInterval = "2"
    val pollTimeout = "10000"

    val sparkConf = new SparkConf().setAppName("UberStream")


    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval.toInt))
    val producerConf = new ProducerConf(
      bootstrapServers = brokers.split(",").toList
    )
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      "spark.kafka.poll.time" -> pollTimeout,
      "spark.streaming.kafka.consumer.poll.ms" -> "8192"
    )

    val model = KMeansModel.load("/user/user01/data/savemodel")
    model.clusterCenters.foreach(println)

    val linesDStream = KafkaUtils.createDirectStream[String, String](ssc, kafkaParams, topicsSet)

    val uDStream = linesDStream.map(_._2).map(_.split(",")).map(p => Uber(p(0), p(1).toDouble, p(2).toDouble, p(3)))
    
    uDStream.foreachRDD{ rdd =>

      // There exists at least one element in RDD
      if (!rdd.isEmpty) {
        val count = rdd.count
        println("count received " + count)
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._
        import org.apache.spark.sql.functions._
        import org.apache.spark.sql.types._

        val df = rdd.toDF()
        // Display the top 20 rows of DataFrame
        println("uber data")
        df.show()
        df.registerTempTable("uber")

        val featureCols = Array("lat", "lon")
        val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
        val df2 = assembler.transform(df)
        val categories = model.transform(df2)
        categories.show
    //    val res = categories.select(dayofmonth($"dt").alias("day"), $"prediction").groupBy("day", "prediction").count
     //   println("count " + res)
        
         val sRDD: RDD[String] = categories.select("dt", "lat","lon","base", "prediction").toJSON

        sRDD.take(2).foreach(println)

        // val sRDD: RDD[String] = cdrDF.groupBy("squareId").count().orderBy(desc("count")).map(row => s"squareId: ${row(0)}, count: ${row(1)}")
        sRDD.sendToKafka[StringSerializer](topicp, producerConf)

   
     //   categories.groupBy( "prediction").count().orderBy(desc("count")).show 

        println("sending messages")
      }
    }

    // Start the computation
    println("start streaming")
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()

  }

}