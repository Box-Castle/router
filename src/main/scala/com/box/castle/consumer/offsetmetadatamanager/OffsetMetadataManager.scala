package com.box.castle.consumer.offsetmetadatamanager

import java.io.Closeable

import kafka.api.{OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse}
import kafka.consumer.SimpleConsumer

/**
  * Definition for managing Kafka offset and metadata. Primary methods are to commit and fetch offset and metadata.
  */
// $COVERAGE-OFF$
trait OffsetMetadataManager extends Closeable {

  def close(): Unit

  def commitOffsets(offsetCommitRequest: OffsetCommitRequest): OffsetCommitResponse

  def fetchOffsets(offsetFetchRequest: OffsetFetchRequest): OffsetFetchResponse

}

object OffsetMetadataManager {
  sealed abstract class OffsetMetadataManagerException(msg: String) extends Exception(msg)
  case class OffsetMetadataManagerInitializationException(msg: String) extends OffsetMetadataManagerException(msg)
}

// $COVERAGE-ON$