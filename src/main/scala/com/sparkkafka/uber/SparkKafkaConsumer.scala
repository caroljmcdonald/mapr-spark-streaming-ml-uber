package com.sparkkafka.uber

import org.apache.kafka.clients.consumer.ConsumerConfig

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{ DStream, InputDStream }
import org.apache.spark.streaming.kafka.v09.KafkaUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }

import org.apache.spark.streaming.kafka.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

/*

*/
object SparkKafkaConsumer {

  def main(args: Array[String]) = {
    if (args.length < 1) {
      System.err.println("Usage: SparkKafkaConsumerDemo <topic consume> ")
      System.exit(1)
    }

    val schema = StructType(Array(
      StructField("dt", TimestampType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("prediction", IntegerType, true),
      StructField("base", StringType, true)
    ))
    val groupId = "testgroup"
    val offsetReset = "earliest"
    val pollTimeout = "1000"
    val Array(topicc) = args
    val brokers = "maprdemo:9092" // not needed for MapR Streams, needed for Kafka

    val sparkConf = new SparkConf()
      .setAppName(SparkKafkaConsumer.getClass.getName)
      .set("spark.cores.max", "1")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("~/tmp")

    val topicsSet = topicc.split(",").toSet

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      "spark.kafka.poll.time" -> pollTimeout
    )
    val producerConf = new ProducerConf(
      bootstrapServers = brokers.split(",").toList
    )

    val messagesDStream: InputDStream[(String, String)] = {
      KafkaUtils.createDirectStream[String, String](ssc, kafkaParams, topicsSet)
    }

    val valuesDStream: DStream[String] = messagesDStream.map(_._2)

    valuesDStream.foreachRDD { rdd =>
      // There exists at least one element in RDD
      if (!rdd.isEmpty) {
        val count = rdd.count
        println("count received " + count)
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._
        import org.apache.spark.sql.functions._
        val df = sqlContext.read.schema(schema).json(rdd)
        df.show
        df.registerTempTable("uber")

        df.groupBy("prediction").count().show()
        

        sqlContext.sql("select prediction, count(prediction) as count from uber group by prediction").show

        sqlContext.sql("SELECT hour(uber.dt) as hr,count(prediction) as ct FROM uber group By hour(uber.dt)").show
      }
    }

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
