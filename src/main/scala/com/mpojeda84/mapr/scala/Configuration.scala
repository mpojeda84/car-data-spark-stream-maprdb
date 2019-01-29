package com.mpojeda84.mapr.scala;

case class Configuration(tableName: String, topic: String)

object Configuration {

  def parse(args: Seq[String]): Configuration = parser.parse(args, Configuration.default).get

  def default: Configuration = DefaultConfiguration



  object DefaultConfiguration extends Configuration(
    "path/to/json",
    "/path/to/stream:topic"
  )

  private val parser = new scopt.OptionParser[Configuration]("App Name") {
    head("App Name")

    opt[String]('t', "table")
      .action((t, config) => config.copy(tableName = t))
      .maxOccurs(1)
      .text("MapR-DB table name to write stats to")

    opt[String]('n', "topic")
      .action((s, config) => config.copy(topic = s))
      .text("Topic where Kafka Producer is writing to")
  }
}