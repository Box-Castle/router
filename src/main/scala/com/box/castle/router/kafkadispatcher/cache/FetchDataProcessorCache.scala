package com.box.castle.router.kafkadispatcher.cache

import com.box.castle.batch.CastleMessageBatch
import kafka.common.TopicAndPartition

/**
 * Keeps a cache per topic partition.  Will properly resize each cache if we add or remove topic partitions.
 */
private[kafkadispatcher] class FetchDataProcessorCache private(
    cacheMap: Map[TopicAndPartition, Cache], maxSizeInBytes: Long) {

  require(maxSizeInBytes > 0, "Cache must have more than 0 bytes to use")

  def add(topicAndPartition: TopicAndPartition, batch: CastleMessageBatch): FetchDataProcessorCache = {
    cacheMap.get(topicAndPartition) match {
      case Some(cache) => new FetchDataProcessorCache(cacheMap + (topicAndPartition -> cache.add(batch)), maxSizeInBytes)
      case None => {
        val newCache = Cache(batch, maxSizeInBytes / (cacheMap.size + 1))
        // If we are adding cache for a new topic and partition, then we have to
        // resize the existing caches to accommodate the new cache
        FetchDataProcessorCache.resize(cacheMap + (topicAndPartition -> newCache), maxSizeInBytes)
      }
    }
  }

  def get(topicAndPartition: TopicAndPartition, offset: Long, bufferSize: Int = 0): Option[CastleMessageBatch] =
    cacheMap.get(topicAndPartition).flatMap(cache => cache.getAll(offset, bufferSize))

  def setMaxSizeInBytes(newMaxSizeInBytes: Long): FetchDataProcessorCache = {
    if (newMaxSizeInBytes == maxSizeInBytes)
      this
    else
      FetchDataProcessorCache.resize(cacheMap, newMaxSizeInBytes)
  }

  override def toString: String = {
    cacheMap.toString()
  }
}

private[kafkadispatcher]
object FetchDataProcessorCache {

  def apply(maxSizeInBytes: Long) = new FetchDataProcessorCache(Map.empty[TopicAndPartition, Cache], maxSizeInBytes)

  private def resize(cacheMap: Map[TopicAndPartition, Cache], maxSizeInBytes: Long): FetchDataProcessorCache = {
    if (cacheMap.nonEmpty) {
      val sizePerTopicAndPartition = maxSizeInBytes / cacheMap.size
      new FetchDataProcessorCache(cacheMap.map {
        case (topicAndPartition, cache) => (topicAndPartition, cache.setMaxSizeInBytes(sizePerTopicAndPartition))
      }, maxSizeInBytes)
    }
    else {
      // Empty MessageFetcherCache
      new FetchDataProcessorCache(Map.empty[TopicAndPartition, Cache], maxSizeInBytes)
    }
  }
}

