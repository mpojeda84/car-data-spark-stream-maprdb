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
import com.mapr.db.spark.streaming.MapRDBSourceConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}


object Application {

  def main (args: Array[String]): Unit = {

    //val argsConfiguration = Configuration.parse(args)

    val spark = SparkSession.builder.appName("Car Data Transformation").getOrCreate

    import spark.implicits._

    val stream = spark.readStream.format("kafka").option("failOnDataLoss", false).option("kafka.bootstrap.servers", "").option("subscribe", "/user/mapr/car-stream:obd_msg").option("startingOffsets", "earliest").load()

    val documents = stream.select("value").as[String].map(toJsonWithId)

    documents.createOrReplaceTempView("raw_data")

    val all = spark.sql("SELECT VIN AS `vin`, first(make) AS `make`, first(`year`) AS `year`, avg(cast(`speed` AS Double)) AS `avgSpeed`, max(cast(`instantFuelEconomy` AS Double)) AS `bestFuelEconomy`, avg(cast(`instantFuelEconomy` AS Double)) AS `totalFuelEconomy` FROM raw_data GROUP BY vin")

    val query = all.writeStream
      .format(MapRDBSourceConfig.Format)
      .option(MapRDBSourceConfig.TablePathOption, "/user/mapr/car-table-direct-3")
      .option(MapRDBSourceConfig.CreateTableOption, false)
      .option(MapRDBSourceConfig.IdFieldPathOption, "vin")
      .option("checkpointLocation", "/user/mapr/temp")
      .outputMode("complete")
      .start()

    query.awaitTermination()

  }

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
