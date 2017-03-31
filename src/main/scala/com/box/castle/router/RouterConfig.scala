package com.box.castle.router

import com.box.castle.consumer.CastleSimpleConsumer
import com.box.castle.router.RouterConfig._

import scala.concurrent.duration.FiniteDuration




case class RouterConfig(maxWaitTime: FiniteDuration = DefaultMaxWait,
                        minBytes: Int = DefaultMinBytes) {

}

object RouterConfig {
  val DefaultMaxWait = CastleSimpleConsumer.DefaultMaxWait
  val DefaultMinBytes = CastleSimpleConsumer.DefaultMinBytes
  val DefaultConfig = RouterConfig()
}