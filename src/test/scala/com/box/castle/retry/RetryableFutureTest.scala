package com.box.castle.retry

import java.util.concurrent.TimeUnit

import com.box.castle.retry.strategies.{TruncatedBinaryExponentialBackoffStrategy}
import org.specs2.mutable.Specification

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future}

/**
 * Created by dgrenader on 11/26/14.
 */
class RetryableFutureTest extends Specification {

  class CustomException(val x: Int) extends Exception

  def testRetryableFutureHelper(maxRetries: Int) = {
    var numTries = 0
    var lastPreRetryNumRetries = -1

    val myStrategy = TruncatedBinaryExponentialBackoffStrategy(slotSize=FiniteDuration(3, "ms"),
      maxSlots=5, maxRetries=maxRetries)

    val retryConfig = RetryConfig(strategy = myStrategy,
      preRetry = (numRetries, delay, t) => lastPreRetryNumRetries = numRetries)

    val f: Future[String] = RetryableFuture({
      numTries += 1
      throw new CustomException(5150)
    }, retryConfig)

    Await.result(f, FiniteDuration(600, TimeUnit.SECONDS)) must throwA[RetriesExceededException].like {
      case e: RetriesExceededException =>
        e.getMessage must startWith(s"RetryAsync exceeded the max number of retries: $maxRetries")
      case _ =>
        throw new Exception("This is not expected")
    }

    // This includes the original try and all the retries
    numTries must_== maxRetries + 1

    if (maxRetries == 0)
      lastPreRetryNumRetries must_== -1
    else
    // The last call to preRetry will always pass 1 less than the maxRetries possible
    // For exampe if maxRetries is 1, we will retry once, and preRetry will be called once with numRetries = 0
    // because it gets called BEFORE we do the actual retry
      lastPreRetryNumRetries must_== maxRetries - 1
  }

  "RetryableFuture" should {
    "retry as many times as specified by maxRetries in the config provided to it" in {
      testRetryableFutureHelper(0)
      testRetryableFutureHelper(1)
      testRetryableFutureHelper(2)
      testRetryableFutureHelper(7)
    }
  }

  "RetryableFuture" should {
    "fail with the original exception when it is not retryable" in {
      var numTries = 0
      var preRetryNumRetries = -1

      val retryConfig = RetryConfig(isRetryable = (t) => false,
                                    preRetry = (numRetries, delay, t) => preRetryNumRetries = numRetries)

      val f: Future[String] = RetryableFuture({
        numTries += 1
        throw new CustomException(5150)
      }, retryConfig)


      Await.result(f, FiniteDuration(600, TimeUnit.SECONDS)) must throwA[CustomException].like {
        case e: CustomException => e.x must_== 5150
        case _ => throw new Exception("This is not expected")
      }

      // There should only be 1 try
      numTries must_== 1

      // preRetry was never called
      preRetryNumRetries must_== -1
    }
  }

}
