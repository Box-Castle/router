package com.box.castle.router.kafkadispatcher.cache

import com.box.castle.batch.CastleMessageBatch
import com.box.castle.collections.immutable.LinkedHashMap
import com.box.castle.router.kafkadispatcher.cache.CacheWithData.shrink

import scala.annotation.tailrec

/**
 * An immutable implementation of Cache with data in it.
 */
private[cache] class CacheWithData private(val data: LinkedHashMap[Long, CastleMessageBatch],
                                           val currentSizeInBytes: Long,
                                           val maxSizeInBytes: Long) extends Cache {

  assert(currentSizeInBytes <= maxSizeInBytes)
  assert(data.size > 0)
  // These are safe to do since data is guaranteed to have at least one element
  val minOffset: Long = data.head._2.offset
  val maxOffset: Long = data.last._2.maxOffset

  def setMaxSizeInBytes(newMaxSizeInBytes: Long): Cache = {
    if (maxSizeInBytes == newMaxSizeInBytes) {
      this
    }
    else if  (currentSizeInBytes < newMaxSizeInBytes) {
      new CacheWithData(data, currentSizeInBytes, newMaxSizeInBytes)
    }
    else {
      // Our current size in bytes exceeds the new max size, so we have to get rid of old cache entries
      shrink(data, currentSizeInBytes, newMaxSizeInBytes)
    }
  }

  def add(batch: CastleMessageBatch): Cache = {
    if (batch.sizeInBytes <= maxSizeInBytes && batch.offset > maxOffset) {
      // If the batch size is at or below our maximum size, we can put it in the cache
      shrink(data + (batch.offset -> batch), currentSizeInBytes + batch.sizeInBytes, maxSizeInBytes)
    }
    else {
      this
    }
  }

  /**
   * Returns back a CastleMessageBatch for the given offset.  This works even if the offset happens to be
   * in the middle of a batch and is not exactly aligned with a batch since each CastleMessageBatch consists of
   * multiple messages.  For example, say we have two CastleMessageBatches each with a few messages:
   *    cmb1 = CastleMessageBatch consisting of: Message @ offset 21, Message @ offset 22, Message @ offset 23
   *    cmb2 = CastleMessageBatch consisting of: Message @ offset 24, Message @ offset 25
   *
   * These two messages batches are in our data map in this form:
   *    data = 21 -> cmb1, 24 -> cmb2
   *
   * Then get will behave as such:
   *    get(20) = None
   *    get(21) = Some(CastleMessageBatch(msg @ 21, msg @ 22, msg @ 23))
   *    get(22) = Some(CastleMessageBatch(msg @ 22, msg @ 23))
   *    get(23) = Some(CastleMessageBatch(msg @ 23))
   *    get(24) = Some(CastleMessageBatch(msg @ 24, msg @ 25)
   *    get(25) = Some(CastleMessageBatch(msg @ 25)
   *    get(26) = None
   *
   * @return
   */
  def get(offset: Long): Option[CastleMessageBatch] = {
    // Most of the time we will hit the get(offset) portion of this call
    // The orElse part will only happen at the beginning of the cache warmup when we have to "align" our
    // batches, and must therefore slice into them

    // First we try to get the CastleMessageBatch directly from our data map associated with the offset
    data.get(offset).orElse {
      // Otherwise we will look inside each CastleMessageBatch and try to "slice" out the messages
      // from within the batch, but we can only do this if the offset falls within our range of the minimum and
      // maximum offset we have across all the CastleMessageBatches within our data map
      if (offset >= minOffset && offset <= maxOffset)
        // We iterate over all of the castle message batches we have in data and attempt to
        // create a batch from the given offset if it happens to fall inside that batch
        // createBatchFromOffset returns Some(CastleMessageBatch) if it is successful and None otherwise.
        findOption(data.values)(castleMessageBatch => castleMessageBatch.createBatchFromOffset(offset))
      else
        None
    }
  }

  /**
   * findOption iterates through an Iterable of A and applies function f to each value of type A
   * which returns back an Option[B]
   * If Option[B] is not empty findOption will return that as the result,
   * Otherwise findOption continues going through the remaining values.
   * If findOption exhausts all values without encounering a Some value, it returns None
   */
  @tailrec
  private def findOption[A,B](values: Iterable[A])(f: A => Option[B]): Option[B] =
    values.headOption match {
      case Some(h) =>
        f(h) match {
          case Some(v) => Some(v)
          case None => findOption(values.tail)(f)
        }
      case None => None
    }
}

private[cache] object CacheWithData {

  /**
   * Will keep kicking out old batches until the current data is smaller than the given max size in bytes.
   * If all the items in data fit into the target max size already, then this is a no-op.
   *
   * @param targetMaxSizeInBytes - the maximum size in bytes that the data can contain
   * @return
   */
  @tailrec
  private def shrink(data: LinkedHashMap[Long, CastleMessageBatch],
                     currentSizeInBytes: Long,
                     targetMaxSizeInBytes: Long): Cache = {
    if (currentSizeInBytes > targetMaxSizeInBytes) {
      val (_, castleMessageBatch, newData) = data.removeHead()
      assert(newData.size == 0 || castleMessageBatch.offset < newData.head._2.offset)
      shrink(newData, currentSizeInBytes - castleMessageBatch.sizeInBytes, targetMaxSizeInBytes)
    }
    else {
      if (data.size > 0)
        new CacheWithData(data, currentSizeInBytes, targetMaxSizeInBytes)
      else
        new EmptyCache(targetMaxSizeInBytes)
    }
  }

  def apply(batch: CastleMessageBatch, maxSizeInBytes: Long): CacheWithData = {
    new CacheWithData(LinkedHashMap(batch.offset -> batch), batch.sizeInBytes, maxSizeInBytes)
  }
}
