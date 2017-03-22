package com.box.castle.consumer

import java.util.concurrent.TimeUnit

import com.box.kafka.Broker
import com.box.castle.consumer.CastleSimpleConsumerFactory.{DefaultBrokerTimeout, DefaultBufferSizeBytes, DefaultSimpleConsumerFactory}
import com.box.castle.consumer.offsetmetadatamanager.{ZookeeperOffsetMetadataManagerFactory, OffsetMetadataManager, OffsetMetadataManagerFactory}

import scala.concurrent.duration.FiniteDuration

class CastleSimpleConsumerFactory(clientId: ClientId,
                                  zookeeperOffsetMetadataManagerFactory: Option[ZookeeperOffsetMetadataManagerFactory] = None,
                                  brokerTimeout: FiniteDuration = DefaultBrokerTimeout,
                                  bufferSizeBytes: Int = DefaultBufferSizeBytes,
                                  simpleConsumerFactory: SimpleConsumerFactory = DefaultSimpleConsumerFactory,
                                  useKafkaOffsetMetadataManager: Map[ConsumerId, Boolean] = Map.empty) {

  def create(broker: Broker): CastleSimpleConsumer = {
    new CastleSimpleConsumer(broker, brokerTimeout, bufferSizeBytes,
      clientId, simpleConsumerFactory, zookeeperOffsetMetadataManagerFactory, useKafkaOffsetMetadataManager)
  }
}

object CastleSimpleConsumerFactory {
  val DefaultClientId = classOf[CastleSimpleConsumer].getCanonicalName
  val DefaultBrokerTimeout = FiniteDuration(60, TimeUnit.SECONDS)
  val DefaultBufferSizeBytes = 1 * 1024 * 1024
  val DefaultSimpleConsumerFactory = new SimpleConsumerFactory()
}



