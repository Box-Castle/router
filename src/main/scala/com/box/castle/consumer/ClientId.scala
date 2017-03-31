package com.box.castle.consumer

/**
  * Represents a host or set of hosts accessing Kafka.
  */
case class ClientId(private val valueRaw: String) {
  val value = valueRaw.replaceAll("[^A-Za-z0-9-_]", "_")

  override def toString: String = {
    value
  }
}
