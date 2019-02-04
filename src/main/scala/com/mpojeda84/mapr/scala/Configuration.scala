package com.mpojeda84.mapr.scala;

case class Configuration(tableName: String, topic: String, transformed: String, community: String)

object Configuration {

  def parse(args: Seq[String]): Configuration = parser.parse(args, Configuration.default).get

  def default: Configuration = DefaultConfiguration

  object DefaultConfiguration extends Configuration(
    "path/to/json",
    "/path/to/stream:topic",
    "/obd/car-data-transformed",
    "/user/mapr/tables/car-community-values"
  )

  private val parser = new scopt.OptionParser[Configuration]("App Name") {
    head("App Name")

    opt[String]('t', "table")
      .action((t, config) => config.copy(tableName = t))
      .maxOccurs(1)
      .text("MapR-DB table name to write stats to")

    opt[String]('r', "transformed")
      .action((t, config) => config.copy(transformed = t))
      .maxOccurs(1)
      .text("MapR-DB table name to write results to")

    opt[String]('n', "topic")
      .action((s, config) => config.copy(topic = s))
      .text("Topic where Kafka Producer is writing to")

    opt[String]('c', "community")
      .action((s, config) => config.copy(community = s))
      .text("Community table")
  }
}