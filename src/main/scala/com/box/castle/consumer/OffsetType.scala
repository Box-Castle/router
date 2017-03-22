package com.box.castle.consumer

import kafka.api.OffsetRequest

// $COVERAGE-OFF$

/**
 * Represents an offset type within a Kafka topic, which can either be the earliest offset or the latest offset
 */
sealed trait OffsetType {
  private[consumer] def toLong: Long
}

case object LatestOffset extends OffsetType {
  private[consumer] def toLong: Long = OffsetRequest.LatestTime
}

case object EarliestOffset extends OffsetType {
  private[consumer] def toLong: Long = OffsetRequest.EarliestTime
}

// $COVERAGE-ON$
