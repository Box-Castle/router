package com.box.castle.router.proxy

import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.kafkadispatcher.messages.ResizeCache
import com.box.kafka.Broker
import com.box.castle.consumer.CastlePartitionMetadata
import org.slf4s.Logging
import kafka.common.TopicAndPartition

import scala.compat.Platform
import scala.util.Random

/**
 * The KafkaDispatcherProxyPool consists of a map of brokers to their corresponding KafkaDipatcherActor
 * so that requests can be routed to the appropriate KafkaDipatcherActor associated with a given broker.
 */
private[router]
class KafkaDispatcherProxyPool(kafkaDispatcherProxyFactory: KafkaDispatcherProxyFactory,
                   cacheSizeInBytes: Long,
                   metricsLogger: MetricsLogger) extends Logging {
  private val rng = new Random(Platform.currentTime)

  private var pool = Map.empty[Broker, KafkaDispatcherProxy]
  private var leaderMap = Map.empty[TopicAndPartition, Broker]

  /**
   * Updates the pool of kafka dispatchers to be the same as the set of given brokers
   * @param brokers
   * @return
   */
  def updateBrokers(brokers: Set[Broker]): Unit = {
    require(brokers.nonEmpty, "must provide at least one broker when updating")

    val brokersToRemove = pool.keySet -- brokers
    val brokersToAdd = brokers -- pool.keySet

    log.info(s"Updating brokers." +
      s"\nNew brokers: $brokers" +
      s"\nExisting brokers: ${pool.keySet}" +
      s"\nBrokers to kill: $brokersToRemove" +
      s"\nBrokers to add: $brokersToAdd")

    addBrokers(brokersToAdd)
    removeBrokers(brokersToRemove)

    assert(brokers.size == pool.size)
  }

  /**
   * Returns a KafkaDispatcherProxy option for the given topic and partition.  If
   * there is no leader associated with the given topic and partition, this method
   * will return None
   */
  def get(topicAndPartition: TopicAndPartition): Option[KafkaDispatcherProxy] = {
    leaderMap.get(topicAndPartition).map(leaderBroker => get(leaderBroker))
  }

  /**
   * Returns a KafkaDispatcherProxy associated with the given Broker, will create the
   * KafkaDispatcherProxy if necessary
   */
  def get(broker: Broker): KafkaDispatcherProxy = {
    pool.get(broker) match {
      case Some(dispatcherProxy) => dispatcherProxy
      case None => {
        addBrokers(Set(broker))
        pool(broker)
      }
    }
  }

  def removeBroker(broker: Broker): Unit = {
    removeBrokers(Set(broker))
  }

  private def removeBrokers(brokers: Set[Broker]): Unit = {
    log.info(s"Removing brokers: $brokers")
    pool.filterKeys(brokers.contains).values.foreach(proxy => proxy.kill())
    pool = pool -- brokers
    if (pool.nonEmpty) {
      val cacheSizePerBrokerInBytes = cacheSizeInBytes / pool.size

      val resizeCacheMsg = ResizeCache(cacheSizePerBrokerInBytes)
      pool.values.foreach(proxy => proxy ! resizeCacheMsg)
    }
  }

  // Will strictly add these brokers to the pool of dispatcher proxies
  private def addBrokers(brokers: Set[Broker]): Unit = {
    log.info(s"Adding brokers: $brokers")
    if (brokers.nonEmpty) {
      val cacheSizePerBrokerInBytes = cacheSizeInBytes / (pool.size + brokers.size)

      // We need to make sure we resize the cache for the existing kafka dispatchers
      val resizeCacheMsg = ResizeCache(cacheSizePerBrokerInBytes)
      pool.values.foreach(proxy => proxy ! resizeCacheMsg)

      brokers.foreach(broker =>
        pool = pool + (broker -> kafkaDispatcherProxyFactory.create(broker, cacheSizePerBrokerInBytes)))
    }
  }

  /**
   * Returns a random KafkaDispatcherProxy from the pool
   */
  def random: KafkaDispatcherProxy = {
    assert(pool.nonEmpty)
    rng.shuffle(pool.values).head
  }

  def size: Int = pool.size

  /**
   * Updates the leader map to match the given topic metadata
   */
  def updateLeaderMap(topicMetadata: Map[TopicAndPartition, CastlePartitionMetadata]): Unit = {
    leaderMap = topicMetadata.collect {
      case (topicAndPartition, CastlePartitionMetadata(Some(leader), _, _)) => topicAndPartition -> leader
    }

    val currentBrokers = topicMetadata.collect {
      case (_, CastlePartitionMetadata(Some(leader), _, _)) => leader
    }.toSet

    updateBrokers(currentBrokers)
  }
}

