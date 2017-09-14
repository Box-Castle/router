package com.box.castle.router.kafkadispatcher.cache

import com.box.castle.batch.CastleMessageBatch
import kafka.common.TopicAndPartition

/**
 * Keeps a cache per topic partition.  Will properly resize each cache if we add or remove topic partitions.
 */
private[kafkadispatcher] class FetchDataProcessorCache private(
    cacheMap: Map[TopicAndPartition, Cache], maxSizeInBytes: Long, bufferSize: Int) {

  require(maxSizeInBytes > 0, "Cache must have more than 0 bytes to use")
  require(bufferSize > 0, "Fetch BufferSize must be larger than 0 bytes to fetch from the cache")

  def add(topicAndPartition: TopicAndPartition, batch: CastleMessageBatch): FetchDataProcessorCache = {
    cacheMap.get(topicAndPartition) match {
      case Some(cache) => new FetchDataProcessorCache(cacheMap + (topicAndPartition -> cache.add(batch)), maxSizeInBytes, bufferSize)
      case None => {
        val newCache = Cache(batch, maxSizeInBytes / (cacheMap.size + 1), bufferSize)
        // If we are adding cache for a new topic and partition, then we have to
        // resize the existing caches to accommodate the new cache
        FetchDataProcessorCache.resize(cacheMap + (topicAndPartition -> newCache), maxSizeInBytes, bufferSize)
      }
    }
  }

  def get(topicAndPartition: TopicAndPartition, offset: Long): Option[CastleMessageBatch] =
    cacheMap.get(topicAndPartition).flatMap(cache => cache.get(offset))

  def setMaxSizeInBytes(newMaxSizeInBytes: Long): FetchDataProcessorCache = {
    if (newMaxSizeInBytes == maxSizeInBytes)
      this
    else
      FetchDataProcessorCache.resize(cacheMap, newMaxSizeInBytes, bufferSize)
  }

  override def toString: String = {
    cacheMap.toString()
  }
}

private[kafkadispatcher]
object FetchDataProcessorCache {

  def apply(maxSizeInBytes: Long, bufferSize: Int) = new FetchDataProcessorCache(Map.empty[TopicAndPartition, Cache],
    maxSizeInBytes, bufferSize)

  private def resize(cacheMap: Map[TopicAndPartition, Cache], maxSizeInBytes: Long, bufferSize: Int): FetchDataProcessorCache = {
    if (cacheMap.nonEmpty) {
      val sizePerTopicAndPartition = maxSizeInBytes / cacheMap.size
      new FetchDataProcessorCache(cacheMap.map {
        case (topicAndPartition, cache) => (topicAndPartition, cache.setMaxSizeInBytes(sizePerTopicAndPartition))
      }, maxSizeInBytes, bufferSize)
    }
    else {
      // Empty MessageFetcherCache
      new FetchDataProcessorCache(Map.empty[TopicAndPartition, Cache], maxSizeInBytes, bufferSize)
    }
  }
}

