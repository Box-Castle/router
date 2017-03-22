package com.box.castle.retry

import java.util.{Timer, TimerTask}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Failure

/**
  * RetryAsync wraps any block of code that generates a Future[T] in a retry mechanism controllable
  * by the caller via the given strategy.  It will retry both synchronous and and asynchronous failures.
  *
  * For example:
  *    def foo(): Future[Int] = ...
  *
  * Normally we could call:
  *    foo() onComplete { ... }
  *
  * To make foo() retryable simply call:
  *    RetryAsync(foo()) onComplete { ... }
  */
object RetryAsync {

  private val defaultRetryConfig = RetryConfig()

  /**
    *
    * @param body - this block of code must generate a new Future each time it is called
    * @param retryConfig - an optional retry config, to see the default values please refer to RetryConfig
    * @param executor - an execution context for the future, passed implicitly
    * @tparam T
    * @return Future[T]
    */
  def apply[T](body: => Future[T],
               retryConfig: RetryConfig = defaultRetryConfig)(
      implicit executor: ExecutionContext): Future[T] = {
    val promise = Promise[T]

    def attempt(numRetries: Int): Unit = {
      val bodyFuture: Future[T] = RetrySync(body, retryConfig)
      bodyFuture onComplete {
        case Failure(t) => {
          if (!retryConfig.isRetryable(t)) {
            promise.failure(t)
          } else if (retryConfig.strategy.shouldRetry(numRetries)) {
            val delay = retryConfig.strategy.delay(numRetries)

            // This arbitrary user preRetry call happens on the thread where this onComplete callback is happening
            // therefore it is up to the user to decide how heavy to make the code inside of preRetry
            retryConfig.preRetry(numRetries, delay, t)

            // Start timer in daemon mode so it doesn't prevent our app from shutting down
            new Timer(true).schedule(new TimerTask {
              override def run(): Unit = {
                attempt(numRetries + 1)
              }
            }, delay.toMillis)
          } else {
            promise.failure(
                new RetriesExceededException(
                    s"RetryAsync exceeded the max number of retries: $numRetries",
                    t))
          }
        }
        case result => {
          promise.complete(result)
        }
      }
    }

    // We start off with 0 retries
    attempt(0)
    promise.future
  }

  def apply[T](body: => Future[T],
               preRetry: (Int, FiniteDuration, Throwable) => Unit)(
      implicit executor: ExecutionContext): Future[T] = {
    apply(body, RetryConfig(preRetry = preRetry))
  }

  def apply[T](body: => Future[T], strategy: RetryStrategy)(
      implicit executor: ExecutionContext): Future[T] = {
    apply(body, RetryConfig(strategy = strategy))
  }

  def apply[T](body: => Future[T],
               preRetry: (Int, FiniteDuration, Throwable) => Unit,
               strategy: RetryStrategy)(
      implicit executor: ExecutionContext): Future[T] = {
    apply(body, RetryConfig(preRetry = preRetry, strategy = strategy))
  }

  def apply[T](body: => Future[T],
               strategy: RetryStrategy = defaultBackoffStrategy,
               isRetryable: Throwable => Boolean = defaultIsRetryable,
               preRetry: (Int, FiniteDuration, Throwable) => Unit)(
      implicit executor: ExecutionContext): Future[T] = {
    apply(body,
          RetryConfig(strategy = strategy,
                      isRetryable = isRetryable,
                      preRetry = preRetry))
  }
}
