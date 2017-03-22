package com.box.castle.router.kafkadispatcher

import akka.actor.Props
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.RouterConfig
import com.box.kafka.Broker
import com.box.castle.consumer.CastleSimpleConsumerFactory

class KafkaDispatcherFactory(boxSimpleConsumerFactory: CastleSimpleConsumerFactory,
                             metricsLogger: MetricsLogger,
                             routerConfig: RouterConfig = RouterConfig.DefaultConfig) {

  def props(broker: Broker, cacheSizeInBytes: Long): Props = {
    Props(new KafkaDispatcher(boxSimpleConsumerFactory,
                              broker,
                              cacheSizeInBytes,
                              routerConfig,
                              metricsLogger))
  }
}
