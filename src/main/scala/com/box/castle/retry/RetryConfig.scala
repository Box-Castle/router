package com.box.castle.retry

import scala.concurrent.duration.FiniteDuration

/**
  * Represents a retry configuration.
  *
  * @param strategy - an optional strategy to use for delays and stopping the retries.  Defaults to the
  *                   TruncatedBinaryExponentialBackoffStrategy using the default config.  To see the values for
  *                   the default config of this strategy please refer to TruncatedBinaryExponentialBackoffStrategyConfig.
  * @param isRetryable - an optional method that determines which failures should be retried. If a particular
  *                      Throwable is not retryable, then the retry logic will fail with the Throwable as it normally
  *                      would. It is not wrapped in any way.  Defaults to retrying all NonFatals.
  * @param preRetry - an optional function to execute before a retry, it is useful for logging/metrics.
  *                   This function is passed the number of retries we have attempted so far and the cause of
  *                   the failure. The first time this function is called, it will be passed numRetries = 0 because
  *                   it happens BEFORE the actual retry.  Defaults to a no-op.
  */
case class RetryConfig(strategy: RetryStrategy = defaultBackoffStrategy,
                       isRetryable: Throwable => Boolean = defaultIsRetryable,
                       preRetry: (Int, FiniteDuration, Throwable) => Unit =
                         defaultPreRetry) {}
