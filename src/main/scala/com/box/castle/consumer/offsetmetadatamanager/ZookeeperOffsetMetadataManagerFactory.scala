package com.box.castle.consumer.offsetmetadatamanager

import java.nio.file.Path
import java.util.concurrent.TimeUnit._
import kafka.consumer.SimpleConsumer
import org.apache.curator.RetryPolicy
import org.apache.curator.retry.ExponentialBackoffRetry

import scala.concurrent.duration.{FiniteDuration, Duration}

class ZookeeperOffsetMetadataManagerFactory(rootNameSpace: Path,
                                            connectionEndpoint: String,
                                            retryPolicy: RetryPolicy = new ExponentialBackoffRetry(1500, 3),
                                            initialConnectionTimeout: Duration = FiniteDuration(10, SECONDS),
                                            connectionTimeout: Duration = FiniteDuration(10, SECONDS),
                                            sessionTimeout: Duration = FiniteDuration(30, SECONDS))
  extends OffsetMetadataManagerFactory {

  def create(consumer: SimpleConsumer): ZookeeperOffsetMetadataManager = {
    new ZookeeperOffsetMetadataManager(rootNameSpace, connectionEndpoint, retryPolicy,
      initialConnectionTimeout, connectionTimeout, sessionTimeout)
  }
}