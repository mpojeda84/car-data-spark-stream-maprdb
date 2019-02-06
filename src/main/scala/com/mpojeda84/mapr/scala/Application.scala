package com.mpojeda84.mapr.scala

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.ojai.joda.DateTime
import com.mapr.db.spark._
import com.mpojeda84.mapr.scala.config.Configuration
import com.mpojeda84.mapr.scala.model.CarDataInstant


object Application {

  def main (args: Array[String]): Unit = {

    val argsConfiguration = Configuration.parse(args)

    val config = new SparkConf().setAppName("Car data raw ingestion")

    val sc = new SparkContext(config)
    implicit val ssc: StreamingContext = new StreamingContext(sc, Milliseconds(500))

    println("### RUNNING ###")
    println(argsConfiguration.tableName)
    println(argsConfiguration.topic)

    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](List(argsConfiguration.topic), kafkaParameters)
    val directStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy)

    directStream.map(_.value())
      .map(toJsonWithId)
      .foreachRDD { rdd =>
        rdd.saveToMapRDB(argsConfiguration.tableName)
      }

    ssc.start()
    ssc.awaitTermination()

  }

  private def kafkaParameters = Map(
    ConsumerConfig.GROUP_ID_CONFIG -> ("connected-car" + DateTime.now().toString),
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
  )

  private def toJsonWithId(csvLine: String): CarDataInstant = {
    val values = csvLine.split(",").map(_.trim)

    val id = values(1) + values(5) + values(6);

    CarDataInstant(
      id,
      values(1),
      values(2),
      values(3),
      values(4),
      values(5),
      values(6),
      values(7),
      values(8),
      values(9),
      values(10),
      values(11),
      values(12),
      values(13),
      values(14),
      values(15),
      values(16),
      values(17),
      values(18),
      values(19)
    )

  }

}