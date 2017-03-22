package com.box.castle.retry.strategies

import java.util.concurrent.TimeUnit
import com.box.castle.common.Require
import com.box.castle.retry.RetryStrategy
import com.box.castle.retry.strategies.RandomMinMaxStrategy.{DefaultMaxDelay, DefaultMaxRetries, DefaultMinDelay}

import scala.concurrent.duration.FiniteDuration
import scala.util.Random

case class RandomMinMaxStrategy(minDelay: FiniteDuration = DefaultMinDelay,
                                maxDelay: FiniteDuration = DefaultMaxDelay,
                                maxRetries: Int = DefaultMaxRetries)
    extends RetryStrategy {

  Require.positiveDuration(minDelay, "minDelay")
  Require.positiveDuration(maxDelay, "maxDelay")
  require(maxRetries >= 0)

  def shouldRetry(numRetries: Int): Boolean = {
    numRetries < maxRetries
  }

  /**
    * RandomMinMaxBackoffStrategy ignores the numRetries parameter and returns a random duration
    * between min and max delay.
    * @param numRetries
    */
  def delay(numRetries: Int): FiniteDuration = {
    val min = minDelay.toMillis
    val max = maxDelay.toMillis
    val delay = min + Random.nextInt((max - min).toInt + 1)
    FiniteDuration(delay, TimeUnit.MILLISECONDS)
  }

  def delay(): FiniteDuration = {
    delay(0)
  }
}

object RandomMinMaxStrategy {
  val DefaultMinDelay = FiniteDuration(30, TimeUnit.SECONDS)
  val DefaultMaxDelay = FiniteDuration(60, TimeUnit.SECONDS)
  val DefaultMaxRetries = Integer.MAX_VALUE
  val DefaultStrategy = new RandomMinMaxStrategy(DefaultMinDelay,
                                                 DefaultMaxDelay,
                                                 DefaultMaxRetries)
}
