package com.box.castle.router.proxy

import akka.actor.ActorContext
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.kafkadispatcher.KafkaDispatcherFactory
import org.slf4s.Logging

/**
 * Created by dgrenader on 5/10/15.
 */
class KafkaDispatcherProxyPoolFactory(kafkaDispatcherFactory: KafkaDispatcherFactory,
                          cacheSizeInBytes: Long,
                          metricsLogger: MetricsLogger) extends Logging {

  def create(context: ActorContext): KafkaDispatcherProxyPool = {
    val kafkaDispatcherProxyFactory = new KafkaDispatcherProxyFactory(kafkaDispatcherFactory, context)
    new KafkaDispatcherProxyPool(kafkaDispatcherProxyFactory, cacheSizeInBytes, metricsLogger)
  }
}
