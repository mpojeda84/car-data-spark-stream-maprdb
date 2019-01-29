package com.mpojeda84.mapr.scala

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.ojai.joda.DateTime;
import com.mapr.db._
import com.mapr.db.spark._
import com.mapr.db.spark.impl._
import com.mapr.db.spark.streaming._
import com.mapr.db.spark.sql._


object Application {

  def main (args: Array[String]): Unit = {

    val argsConfiguration = Configuration.parse(args)

    val config = new SparkConf().setAppName("Car Instant Data Ingestion")

    val sc = new SparkContext(config)
    implicit val ssc: StreamingContext = new StreamingContext(sc, Milliseconds(500))

    println("### RUNNING ###")
    println(argsConfiguration.tableName)
    println(argsConfiguration.topic)


    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](List(argsConfiguration.topic), kafkaParameters)
    val directStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy)

    val recordsDirectStream: DStream[CarDataInstant] =
      directStream.map(_.value())
      .map(toJsonWithId)

    recordsDirectStream.print(3)

    //recordsDirectStream.saveToMapRDB(argsConfiguration.tableName, false, false, "_id")

    ssc.start()
    ssc.awaitTermination()
  }


  private def kafkaParameters = Map(
    ConsumerConfig.GROUP_ID_CONFIG -> "connected-car",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true"
  )

  private def toJsonWithId(csvLine: String): CarDataInstant = {
    val values = csvLine.split(",").map(_.trim)

    val id = values(0) + DateTime.now().toString();

    CarDataInstant(
      id,
      values(0),
      values(1),
      "",
      values(2).toInt,
      values(3).toDouble,
      values(4).toDouble,
      values(5).toDouble,
      values(6).toDouble,
      values(7).toDouble,
      values(8).toDouble,
      values(10).toInt,
      0,
      ""
    )

  }

}
