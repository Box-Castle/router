package com.box.castle

import com.box.castle.retry.strategies.TruncatedBinaryExponentialBackoffStrategy

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

package object retry {

  val defaultBackoffStrategy = TruncatedBinaryExponentialBackoffStrategy()

  val defaultIsRetryable = (t: Throwable) =>
    t match {
      case NonFatal(e) => true
      case _ => false
  }

  val defaultPreRetry = (_: Int, _: FiniteDuration, _: Throwable) => ()
}
