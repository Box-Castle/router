package com.box.castle.retry.strategies

import java.util.concurrent.TimeUnit
import com.box.castle.common.Require
import com.box.castle.retry.RetryStrategy
import com.box.castle.retry.strategies.TruncatedBinaryExponentialBackoffStrategy.{DefaultMaxRetries,
  DefaultMaxSlots, DefaultSlotSize}

import scala.concurrent.duration.FiniteDuration
import scala.util.Random

/**
  * Please see http://en.wikipedia.org/wiki/Exponential_backoff for details
  *
  * The default configuration results in a maximum possible wait time of about 2 minutes
  * This can be derived by (pow(2, 10) * 117ms) / 1000 = 119.8 sec / 60 = 2 mins
  *
  * Since are choosing a random number of slots from the range, our average target wait time is 1 minute.
  *
  * The default maximum retries is indefinite in practice.
  *
  * @param slotSize - The size of the slots as a duration
  * @param maxSlots - Affects the maximum number of slots to chose from
  * @param maxRetries - the maximum number of times to retry, this does not include the original try of the code.
  *                     In other words, specifying 1 maximum retry here would result in a total of TWO tries of
  *                     the wrapped code, the original try and the single retry
  */
case class TruncatedBinaryExponentialBackoffStrategy(
    slotSize: FiniteDuration = DefaultSlotSize,
    maxSlots: Int = DefaultMaxSlots,
    maxRetries: Int = DefaultMaxRetries)
    extends RetryStrategy {

  Require.positiveDuration(slotSize, "slotSize")
  require(maxSlots >= 0)
  require(maxRetries >= 0)

  def shouldRetry(numRetries: Int): Boolean = numRetries < maxRetries

  def delay(numRetries: Int): FiniteDuration = {
    // Limit the maximum number of slots, this is the "truncated" part of the algorithm
    val c = math.min(maxSlots, numRetries)
    // The number of slots we can choose from, this is the "binary" part of the algorithm
    val numSlots = math.pow(2, c).toInt
    // Add some randomness so we don't have pulses in the system
    Random.nextInt(numSlots) * slotSize
  }
}

object TruncatedBinaryExponentialBackoffStrategy {
  val DefaultSlotSize = FiniteDuration(117, TimeUnit.MILLISECONDS)
  val DefaultMaxSlots = 10
  val DefaultMaxRetries = Integer.MAX_VALUE
  val DefaultStrategy = new TruncatedBinaryExponentialBackoffStrategy(
      DefaultSlotSize,
      DefaultMaxSlots,
      DefaultMaxRetries)
}
