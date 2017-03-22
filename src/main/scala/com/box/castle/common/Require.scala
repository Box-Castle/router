package com.box.castle.common

import scala.concurrent.duration.Duration

object Require {
  def positiveDuration(duration: Duration, name: String): Unit =
    require(duration.toMillis >= 0, s"$name must be positive or 0")
}
