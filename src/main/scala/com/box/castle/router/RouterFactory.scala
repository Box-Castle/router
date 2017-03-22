package com.box.castle.router

import akka.actor.Props
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.proxy.KafkaDispatcherProxyPoolFactory
import com.box.kafka.Broker

class RouterFactory(kafkaDispatcherProxyPoolFactory: KafkaDispatcherProxyPoolFactory, brokers: Set[Broker], metricsLogger: MetricsLogger)  {
  require(brokers.nonEmpty, "Must provide at least one broker")

  def props(): Props = {
    Props(new Router(kafkaDispatcherProxyPoolFactory, brokers, metricsLogger))
  }
}
