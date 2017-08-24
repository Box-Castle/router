package com.box.castle.router.kafkadispatcher.cache

import com.box.castle.batch.CastleMessageBatch
import com.box.castle.collections.immutable

/**
 * An immutable implementation of Cache with no data in it
 */
private[cache] class EmptyCache(val maxSizeInBytes: Long) extends Cache {

  // Do not access this in an empty cache
  lazy override val currentSizeInBytes: Long = throw new IllegalAccessException()

  // Do not access this in an empty cache
  lazy override val data: immutable.LinkedHashMap[Long, CastleMessageBatch] = throw new IllegalAccessException()

  def setMaxSizeInBytes(newMaxSizeInBytes: Long): Cache = {
    if (maxSizeInBytes == newMaxSizeInBytes)
      this
    else
      new EmptyCache(newMaxSizeInBytes)
  }

  def add(batch: CastleMessageBatch): Cache = {
    if (batch.sizeInBytes <= maxSizeInBytes)
      CacheWithData(batch, maxSizeInBytes)
    else
      this
  }

  def get(offset: Long): Option[CastleMessageBatch] = None

  def getAll(offset: Long, bufferSize: Int): Option[CastleMessageBatch] = None
}
