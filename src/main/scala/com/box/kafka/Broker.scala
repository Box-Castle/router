package com.box.kafka

/**
  * For reasons defying common sense, the Broker class in the Kafka library is private,
  * so we will be defining our own, which is effectively identical to it
  * @param id
  * @param host
  * @param port
  */
case class Broker(id: Int, host: String, port: Int) {
  def connectionString: String = s"$host:$port"
  override def toString = s"id:$id;host:$host;port:$port"
}
