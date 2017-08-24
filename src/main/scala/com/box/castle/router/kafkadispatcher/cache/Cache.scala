package com.box.castle.router.kafkadispatcher.cache

import com.box.castle.batch.CastleMessageBatch
import com.box.castle.collections.immutable.LinkedHashMap

/**
 * Contains a mapping from offset to CastleMessageBatch in sorted order.  Will return a CastleMessageBatch
 * even if the requested offset falls in the middle of a cached CastleMessageBatch.  When adding a new
 * CastleMessageBatch object to the cache, the Cache will only accept batches that have an offset that is higher than
 * the maximum offset already contained in the cache.
 *
 */
private[kafkadispatcher] abstract class Cache {

  val data: LinkedHashMap[Long, CastleMessageBatch]
  val maxSizeInBytes: Long
  val currentSizeInBytes: Long

  require(maxSizeInBytes > 0, "Cache must have more than 0 bytes to use")

  def setMaxSizeInBytes(newMaxSizeInBytes: Long): Cache

  def add(batch: CastleMessageBatch): Cache

  /**
   * Returns a CastleMessageBatch if the given offset is in the Cache.
   * The offset does not have to align exactly with the first offset in a CastleMessageBatch.  We will go through each
   * batch and try to slice a CastleMessageBatch if it contains this offset somewhere inside it, even if it's not
   * associate with the first message.
   * @param offset
   * @return
   */
  def get(offset: Long): Option[CastleMessageBatch]

  /**
    * Tries to return the largest batch possible in case of a cache hit. Returns None if nothing is found.
    * It uses the provided offset as the entry point in the cache and then recursively tries to find batches
    * that start with the nextOffset of the previous batch till the bufferSize is reached.
    * Passing a bufferSize = 0 makes it behave exactly like get(offset).
    * @param offset
    * @param bufferSize
    * @return
    */
  def getAll(offset: Long, bufferSize: Int): Option[CastleMessageBatch]

  override def toString: String = {
    s"Cache(maxSizeInBytes=$maxSizeInBytes,currentSizeInBytes=$currentSizeInBytes,data=$data)"
  }
}

private[kafkadispatcher] object Cache {

  def apply(maxSizeInBytes: Long): Cache = new EmptyCache(maxSizeInBytes)

  def apply(batch: CastleMessageBatch, maxSizeInBytes: Long): Cache = apply(maxSizeInBytes).add(batch)

}

