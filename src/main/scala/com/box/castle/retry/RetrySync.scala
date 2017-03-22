package com.box.castle.retry

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration

/**
  * RetrySync wraps any synchronized code in a retry mechanism controllable by the caller via the given strategy.
  * Since the code is synchronous, RetrySync will cause the thread to sleep.
  */
object RetrySync {

  private val defaultRetryConfig = RetryConfig()

  /**
    * @param body - the code to retry
    * @param retryConfig - an optional retry config, to see the default values please refer to RetryConfig
    * @tparam T
    * @return - T
    */
  def apply[T](body: => T, retryConfig: RetryConfig = defaultRetryConfig): T = {

    @tailrec
    def attempt(numRetries: Int): T = {
      try {
        body
      } catch {
        case t: Throwable => {
          if (!retryConfig.isRetryable(t)) {
            throw t
          } else if (retryConfig.strategy.shouldRetry(numRetries)) {
            val delay = retryConfig.strategy.delay(numRetries)
            retryConfig.preRetry(numRetries, delay, t)
            Thread.sleep(delay.toMillis)
            attempt(numRetries + 1)
          } else {
            throw new RetriesExceededException(
                s"RetrySync exceeded the max number of retries: $numRetries",
                t)
          }
        }
      }
    }

    // We start off with 0 retries
    attempt(0)
  }

  def apply[T](body: => T,
               preRetry: (Int, FiniteDuration, Throwable) => Unit): T = {
    apply(body, RetryConfig(preRetry = preRetry))
  }

  def apply[T](body: => T, strategy: RetryStrategy): T = {
    apply(body, RetryConfig(strategy = strategy))
  }

  def apply[T](body: => T,
               preRetry: (Int, FiniteDuration, Throwable) => Unit,
               strategy: RetryStrategy): T = {
    apply(body, RetryConfig(preRetry = preRetry, strategy = strategy))
  }
}
