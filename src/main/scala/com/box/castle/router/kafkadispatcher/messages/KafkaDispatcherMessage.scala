package com.box.castle.router.kafkadispatcher.messages

import com.box.castle.router.kafkadispatcher.processors.RequesterInfo
import com.box.castle.consumer.{ConsumerId, OffsetType}
import com.box.castle.router.messages.OffsetAndMetadata
import kafka.api._
import kafka.common.TopicAndPartition

/**
 * This file contains the Akka Message API for the KafkaDispatcher
 *
 * KafkaDispatcher understands only messages that are subtypes of this class.
 */
private[router] sealed abstract class KafkaDispatcherMessage

/**
 * All subtypes of KafkaDispatcherCommonMessage are maintenance style messages that can be handled at any time
 */
private[router] sealed abstract class KafkaDispatcherCommonMessage extends KafkaDispatcherMessage

private[router] case class ResizeCache(cacheSizeInBytes: Long) extends KafkaDispatcherCommonMessage

private[router] case class UnexpectedFailure(t: Throwable) extends KafkaDispatcherCommonMessage

private[kafkadispatcher] case class LeaderNotAvailable[T <: DispatchToKafka](requests: Iterable[T])
  extends KafkaDispatcherCommonMessage

private[kafkadispatcher] case class KafkaBrokerUnreachable(t: Throwable) extends KafkaDispatcherCommonMessage

/**
 * All subtypes of DispatchToKafka result in a potential call to Kafka.  These
 * are only used by the Router to communicate with KafkaDispatcher
 */
private[router] sealed abstract class DispatchToKafka extends KafkaDispatcherMessage {
  val requesterInfo: RequesterInfo
}

private[router]
sealed abstract class DispatchToKafkaWithTopicAndPartition extends DispatchToKafka {
  val topicAndPartition: TopicAndPartition
}

private[router] case class DispatchCommitConsumerOffsetToKafka(consumerId: ConsumerId,
                                       topicAndPartition: TopicAndPartition,
                                       offsetAndMetadata: OffsetAndMetadata,
                                       requesterInfo: RequesterInfo) extends DispatchToKafkaWithTopicAndPartition


private[router] case class DispatchFetchConsumerOffsetToKafka(consumerId: ConsumerId,
                                      topicAndPartition: TopicAndPartition,
                                      requesterInfo: RequesterInfo) extends DispatchToKafkaWithTopicAndPartition

private[router] case class DispatchFetchDataToKafka(topicAndPartition: TopicAndPartition,
                               offset: Long,
                               requesterInfo: RequesterInfo) extends DispatchToKafkaWithTopicAndPartition

private[router] case class DispatchFetchOffsetToKafka(offsetType: OffsetType,
                                    topicAndPartition: TopicAndPartition,
                                    requesterInfo: RequesterInfo) extends DispatchToKafkaWithTopicAndPartition

private[router] case class DispatchFetchTopicMetadataToKafka(requestId: Long, requesterInfo: RequesterInfo) extends DispatchToKafka


private[router] sealed abstract class KafkaResponse extends KafkaDispatcherMessage

private[kafkadispatcher] case class CommitConsumerOffsetKafkaResponse(consumerId: ConsumerId,
                        response: OffsetCommitResponse,
                        requests: Map[TopicAndPartition, (OffsetAndMetadata, RequesterInfo)]) extends KafkaResponse

private[kafkadispatcher] case class FetchConsumerOffsetKafkaResponse(consumerId: ConsumerId,
                                 response: OffsetFetchResponse,
                                 requests: Map[TopicAndPartition, Set[RequesterInfo]]) extends KafkaResponse

private[kafkadispatcher] case class FetchDataKafkaResponse(response: FetchResponse,
                        requests: Map[TopicAndPartition, (Long, Set[RequesterInfo])]) extends KafkaResponse

private[kafkadispatcher] case class FetchOffsetKafkaResponse(response: OffsetResponse,
                 requests: Map[TopicAndPartition, (OffsetType, Set[RequesterInfo])]) extends KafkaResponse

private[kafkadispatcher] case class InternalTopicMetadataResponse(response: TopicMetadataResponse) extends KafkaResponse

private[kafkadispatcher] case class UnknownTopicPartition(topicAndPartition: TopicAndPartition) extends KafkaDispatcherCommonMessage

