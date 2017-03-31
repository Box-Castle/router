package com.box.castle.metrics

trait MetricsLogger {

  /**
    * The count implementation must be thread safe since multiple threads can call this method simultaneously
    * @param component
    * @param metricName
    * @param tags
    * @param value
    */
  def count(component: String,
            metricName: String,
            tags: Map[String, String] = Map.empty,
            value: Long = 1): Unit

  /**
    * The time implementation must be thread safe since multiple threads can call this method simultaneously
    * @param component
    * @param metricName
    * @param tags
    * @param nanoSeconds
    */
  def time(component: String,
           metricName: String,
           tags: Map[String, String],
           nanoSeconds: Long): Unit
}

object MetricsLogger {

  // The default logger does not do anything
  val defaultLogger = new MetricsLogger {
    override def count(component: String,
                       metricName: String,
                       tags: Map[String, String],
                       value: Long): Unit = {}

    override def time(component: String,
                      metricName: String,
                      tags: Map[String, String],
                      nanoSeconds: Long): Unit = {}
  }
}
