package com.box.castle.router.mock

import com.box.castle.metrics.MetricsLogger

import scala.collection.mutable
import scala.concurrent.Promise

class MockMetricsLogger extends MetricsLogger {
  val counts = mutable.HashMap[String, Long]()
  var getCountPromises = mutable.HashMap("committer_fatal" -> Promise[Long])

  private[this] val lock = new Object()

  def getCountFor(name: String): Long = {
    lock.synchronized {
      counts.getOrElse(name, 0L)
    }
  }

  def reset(): Unit = {
    lock.synchronized {
      counts.clear()
    }
  }

  override def toString: String = {
    lock.synchronized {
      "MockMetricsLogger: counts = " + counts.toString
    }
  }

  def count(component: String, name: String, pairs: Map[String, String], value: Long): Unit = {
    lock.synchronized {
      counts(name) = value + getCountFor(name)
      getCountPromises.getOrElse(name, Promise[Long]).success(value)
    }
  }

  def time(component: String, name: String, pairs: Map[String, String], nanoSeconds: Long): Unit = {}
}

