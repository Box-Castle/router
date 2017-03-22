package com.box.castle.retry

import scala.concurrent.duration.FiniteDuration

trait RetryStrategy {

  /**
    * Given this many retries, the strategy should specify if we should continue to retry.
    * If this code returns false, a RetriesExceededException will be raised.  The first time this method will be
    * called with numRetries = 0, and incremented by 1 each time thereafter.
    * @param numRetries
    * @return
    */
  def shouldRetry(numRetries: Int): Boolean

  /**
    * Given this many retries, the strategy should specify how long we should delay before the next retry.
    * The first time this method will be called with numRetries = 0, and incremented by 1 each time thereafter.
    * @param numRetries
    * @return
    */
  def delay(numRetries: Int): FiniteDuration
}
