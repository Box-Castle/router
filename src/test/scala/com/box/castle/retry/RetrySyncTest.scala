package com.box.castle.retry

import com.box.castle.retry.strategies.TruncatedBinaryExponentialBackoffStrategy
import org.specs2.mutable.Specification

import scala.concurrent.duration.FiniteDuration

/**
  * Created by dgrenader on 2/19/15.
  */
class RetrySyncTest extends Specification {
  class CustomException(val x: Int) extends Exception

  def testRetrySyncHelper(maxRetries: Int) = {
    var numTries = 0
    var lastPreRetryNumRetries = -1

    val myStrategy = TruncatedBinaryExponentialBackoffStrategy(slotSize=FiniteDuration(3, "ms"),
      maxSlots=5, maxRetries=maxRetries)

    val retryConfig = RetryConfig(strategy = myStrategy,
      preRetry = (numRetries, delay, t) => {
        println(s"test backoff with RetrySync, numRetries = $numRetries, delay = $delay")
        lastPreRetryNumRetries = numRetries
      })
    def f(): String = {
      numTries += 1
      throw new CustomException(5150)
    }

    RetrySync(f(),retryConfig) must throwA[RetriesExceededException].like {
      case e: RetriesExceededException =>
        e.getMessage must startWith(s"RetrySync exceeded the max number of retries: $maxRetries")
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

  "RetrySync" should {
    "retry as many times as specified by maxRetries in the config provided to it" in {
      testRetrySyncHelper(0)
      testRetrySyncHelper(1)
      testRetrySyncHelper(2)
      testRetrySyncHelper(7)
    }
  }
 }
