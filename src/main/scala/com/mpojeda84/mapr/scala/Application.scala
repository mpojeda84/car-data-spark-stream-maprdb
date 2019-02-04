package com.mpojeda84.mapr.scala


//import com.mapr.db.spark._
//import org.apache.spark.sql.functions.{col, lit}
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql._
//import com.mapr.db.spark.sql._

import org.ojai.joda.DateTime
import com.mapr.db.spark._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import com.mapr.db.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object Application {

  def main (args: Array[String]): Unit = {

    val argsConfiguration = Configuration.parse(args)

    val spark = SparkSession.builder.appName("Car Data Transformation").getOrCreate
    implicit val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Milliseconds(500))

    println("### RUNNING ###")
    println(argsConfiguration.tableName)
    println(argsConfiguration.topic)


    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](List(argsConfiguration.topic), kafkaParameters)
    val directStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy)

      directStream.map(_.value())
      .map(toJsonWithId)
      .foreachRDD { rdd => {
        rdd.saveToMapRDB(argsConfiguration.tableName)
        if(rdd.count() > 0)
          updateTransformed(spark, argsConfiguration.tableName, argsConfiguration.transformed, argsConfiguration.community)
      }}

    ssc.start()
    ssc.awaitTermination()

  }

  private def updateTransformed(spark: SparkSession, raw: String, transformed: String, community: String) : Unit = {

    val df = spark.loadFromMapRDB(raw)

    df.persist(StorageLevel.MEMORY_ONLY)

    df.createOrReplaceTempView("raw_data")

    val ndf_vinmakeyear = spark.sql("SELECT VIN AS `vin`, Make AS `make`, `Year` AS `year` FROM raw_data GROUP BY vin, make, year")
    val highestSpeedToday = spark.sql("SELECT DISTINCT `VIN` AS `vin`, max(cast(`speed` AS Double)) AS `highestSpeedToday` FROM raw_data GROUP BY `VIN`, CAST(`hrtimestamp` AS DATE)")
    val highestSpeedThisWeek = spark.sql("SELECT `VIN` AS `vin`, max(cast(`speed` AS Double)) AS `highestSpeedThisWeek` FROM raw_data GROUP BY `vin`")
    val avgSpeed = spark.sql("SELECT `VIN` AS `vin`, avg(cast(`speed` AS Double)) AS `avgSpeed` FROM raw_data GROUP BY `vin`")
    val highestFuelEconomy = spark.sql("SELECT DISTINCT `VIN` AS `vin`, max(cast(`instantFuelEconomy` AS Double)) AS `bestFuelEconomy` FROM raw_data GROUP BY `VIN`")
    val totalFuelEconomy = spark.sql("SELECT DISTINCT `VIN` AS `vin`, avg(cast(`instantFuelEconomy` AS Double)) AS `totalFuelEconomy` FROM raw_data GROUP BY `VIN`")


    val df2 = ndf_vinmakeyear.join(highestSpeedToday, "vin").join(highestSpeedThisWeek, "vin").join(avgSpeed, "vin").join(highestFuelEconomy, "vin").join(totalFuelEconomy, "vin")
    df2.withColumn("_id", col("vin")).saveToMapRDB(transformed)

    val  commAvgSpeed = spark.sql("SELECT avg(`speed`) AS `avgCommunitySpeed` FROM raw_data")
    commAvgSpeed.withColumn("_id", lit("speed")).saveToMapRDB(community)

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

    val id = values(0) + values(4) + values(5);

    CarDataInstant(
      id,
      values(0),
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
      values(16)
    )

  }

}
