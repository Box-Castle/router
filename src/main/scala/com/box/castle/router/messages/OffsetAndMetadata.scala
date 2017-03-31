package com.box.castle.router.messages

import kafka.common.OffsetMetadataAndError
import OffsetAndMetadata.ConsumerMetadataPrefix

/**
 * Encapsulating an offset and the metadata alongside with it
 */
case class OffsetAndMetadata(offset: Long, metadata: Option[String] = None) {
  def asKafkaOffsetMetadataAndError: OffsetMetadataAndError = {
    metadata match {
      case Some(consumerMetadata) => OffsetMetadataAndError(offset, ConsumerMetadataPrefix + consumerMetadata)
      case None =>  OffsetMetadataAndError(offset)
    }
  }
}

//@TODO add test case
object OffsetAndMetadata {
  val ConsumerMetadataPrefix = "V1|CONSUMER_METADATA"
  def apply(kafkaOffsetMetadataAndError: OffsetMetadataAndError):OffsetAndMetadata = {
    val consumerMetadata = if(kafkaOffsetMetadataAndError.metadata.startsWith(ConsumerMetadataPrefix)) {
      //Only fill in the metadata if the raw metadata(read from kafka) starts with the prefix, which means
      //it's managed by castle
      Some(kafkaOffsetMetadataAndError.metadata.substring(ConsumerMetadataPrefix.length))
    } else {
      None
    }
    OffsetAndMetadata(kafkaOffsetMetadataAndError.offset, consumerMetadata)
  }
}

