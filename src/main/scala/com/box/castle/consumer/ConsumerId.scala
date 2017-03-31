package com.box.castle.consumer

/**
  * Represents a unique consumer key which maintains a separate offset within Kafka.
  */
case class ConsumerId(private val valueRaw: String) {
  val value = valueRaw.replaceAll("[^A-Za-z0-9-_]", "_")

  override def toString: String = value

}

object ConsumerId {

  def apply(namespace: String, id: String): ConsumerId = ConsumerId(namespace + "_" + id)

}
