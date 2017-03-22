package com.box.castle.retry

import java.util.concurrent.TimeUnit

import com.box.castle.retry.strategies.TruncatedBinaryExponentialBackoffStrategy
import org.specs2.mutable.Specification

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future}

/**
 * Created by dgrenader on 2/19/15.
 */
class RetryAsyncTest extends Specification {

  val FastConfig = RetryConfig(TruncatedBinaryExponentialBackoffStrategy(
      slotSize=FiniteDuration(3, "ms"),maxSlots=5, maxRetries=7),
    preRetry = (numRetries, delay, t) =>
      println(s"default FastConfig, numRetries = $numRetries, delay = $delay"))

  class CustomException(val x: Int) extends Exception

  class CustomException2(val customString: String) extends Exception

  "RetryAsync" should {
    "should retry both async and sync exceptions" in {
      val DesiredAsyncFailures = 5
      val DesiredSyncFailures = 3
      var numAsyncTries = 0
      var numSyncTries = 0

      def foo(x: Int): Future[String] = {
        numSyncTries += 1
        if (numSyncTries <= DesiredSyncFailures)
          if (x == 9984) throw new CustomException2("Passed in 9984")

        Future({
          numAsyncTries += 1
          if (numAsyncTries < DesiredAsyncFailures) {
            throw new CustomException(3030)
          } else {
            x.toString
          }
        })
      }

      // We expect this code to be retried because the failure happens in the future
      val f = RetryAsync(foo(2219), FastConfig)
      val r = Await.result(f, FiniteDuration(600, TimeUnit.SECONDS))
      r must_== "2219"
      numAsyncTries must_==  5
      numSyncTries must_== 5

      // Reset
      numAsyncTries = 0
      numSyncTries = 0

      // The call to foo(5) here will cause a synchronous error which does not happen in the future and so
      // RetryAsync does not retry it
      val f2 = RetryAsync(foo(9984), FastConfig)
      val r2 = Await.result(f2, FiniteDuration(600, TimeUnit.SECONDS))
      r2 must_== "9984"
      numAsyncTries must_== 5
      numSyncTries must_== 8  // 5 + 3
    }
  }
}
