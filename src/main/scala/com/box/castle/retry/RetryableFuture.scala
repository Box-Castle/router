package com.box.castle.retry

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

/**
  * RetryableFuture is identical to a Future call, but will additionally retry the body with a given strategy.
  * It will retry both synchronous and asynchronous failures.
  *
  * For example:
  * Future({ ... }) onComplete { ... }
  *
  * Becomes:
  * RetryableFuture({ ... }) onComplete { ... }
  *
  */
object RetryableFuture {

  private val defaultRetryConfig = RetryConfig()

  /**
    * Runs the code in the provided body with retries.
    *
    * @param body - the body of the future
    * @param retryConfig - an optional retry config, to see the default values please refer to RetryConfig
    * @param executor - an execution context for the future, passed implicitly
    * @tparam T
    * @return - Future[T]
    */
  def apply[T](body: => T, retryConfig: RetryConfig = defaultRetryConfig)(
      implicit executor: ExecutionContext): Future[T] = {
    RetryAsync(Future(body), retryConfig)
  }

  def apply[T](body: => T,
               preRetry2: (Int, FiniteDuration, Throwable) => Unit)(
      implicit executor: ExecutionContext): Future[T] = {
    RetryAsync(Future(body), preRetry2)
  }

  def apply[T](body: => T, strategy: RetryStrategy)(
      implicit executor: ExecutionContext): Future[T] = {
    RetryAsync(Future(body), RetryConfig(strategy = strategy))
  }

  def apply[T](body: => T,
               preRetry: (Int, FiniteDuration, Throwable) => Unit,
               strategy: RetryStrategy)(
      implicit executor: ExecutionContext): Future[T] = {
    RetryAsync(Future(body),
               RetryConfig(preRetry = preRetry, strategy = strategy))
  }
}
