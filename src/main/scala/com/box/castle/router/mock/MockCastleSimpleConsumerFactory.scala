package com.box.castle.router.mock

import com.box.castle.consumer.offsetmetadatamanager.ZookeeperOffsetMetadataManagerFactory
import com.box.castle.consumer.{CastleSimpleConsumer, CastleSimpleConsumerFactory, ClientId}
import com.box.kafka.Broker

import java.nio.file.{Path, Paths}

class MockCastleSimpleConsumerFactory(consumers: Map[Broker, CastleSimpleConsumer])
  extends CastleSimpleConsumerFactory(ClientId("mocked"),
    Some(new ZookeeperOffsetMetadataManagerFactory(Paths.get("base_path"), "zk_url"))) {

  override def create(broker: Broker): CastleSimpleConsumer = {
    consumers(broker)
  }
}
