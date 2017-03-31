package com.box.castle.router.messages

import akka.actor.ActorRef
import com.box.castle.batch.CastleMessageBatch
import com.box.castle.router.kafkadispatcher.messages._
import com.box.castle.router.kafkadispatcher.processors.RequesterInfo
import com.box.kafka.Broker
import com.box.castle.consumer.{CastlePartitionMetadata, ConsumerId, OffsetType}
import kafka.common.TopicAndPartition

/**
 * This is effectively the public Akka Message API for interacting with the Router
 * All subtypes of this class are requests that the Router can understand
 */
sealed abstract class RouterMessage

/**
 * All subtypes of RouterResult are sent back to the requester interacting with the Router.
 * These messages are usually, but not always, the result of an operation performed against Kafka.
 */
sealed abstract class RouterResult

sealed abstract class RouteToKafkaDispatcher extends RouterMessage {
  private[router] def asDispatchToKafkaMessage(sender: ActorRef): DispatchToKafka
}

trait TimedOutRequest[A <: RouteToKafkaDispatcher] {
  def request: A
  def requestId: Long
}

/**
 * Send this message to RouterActor to fetch data from Kafka.
 *
 * The sender will likely get one of the following messages sent back to it:
 *   FetchData.Success          has a CastleMessageBatch that contains AT LEAST one message
 *   FetchData.OffsetOutOfRange the requested offset is out of range
 *   FetchData.NoMessages       there are no messages at the given offset, this happens
 *                              if the offset was within range, but there were simply no
 *                              messages written there yet
 *   FetchData.UnknownTopicOrPartition - no such topic or partition exists
 *
 * There is no guarantee that any of these responses will be sent.  Mix in RouterRequestManager into the client Actor
 * to handle the lack of this guarantee.
 */
case class FetchData(topicAndPartition: TopicAndPartition, offset: Long) extends RouteToKafkaDispatcher {

  private[router] def asDispatchToKafkaMessage(sender: ActorRef): DispatchFetchDataToKafka =
    DispatchFetchDataToKafka(topicAndPartition, offset, RequesterInfo(sender))
}

object FetchData {
  sealed abstract class Result extends RouterResult

  case class TimedOut(request: FetchData, requestId: Long) extends Result with TimedOutRequest[FetchData]

  sealed abstract class Response extends Result {
    val topicAndPartition: TopicAndPartition
    val offset: Long
  }

  case class Success(topicAndPartition: TopicAndPartition, offset: Long, batch: CastleMessageBatch) extends Response
  case class OffsetOutOfRange(topicAndPartition: TopicAndPartition, offset: Long) extends Response
  case class NoMessages(topicAndPartition: TopicAndPartition, offset: Long) extends Response
  case class UnknownTopicOrPartition(topicAndPartition: TopicAndPartition, offset: Long) extends Response
  case class CorruptMessage(topicAndPartition: TopicAndPartition, offset: Long, nextOffset: Long) extends Response
}

/**
 * Send this message to RouterActor to get the offset for the given topic and partition from Kafka.
 * If successful, the sender will likely be sent a message that is a subtype of FetchOffset.Response.
 * There is no guarantee that any response will be sent.  Mix in RouterRequestManager into the client Actor
 * to handle the lack of this guarantee.
 */
case class FetchOffset(offsetType: OffsetType, topicAndPartition: TopicAndPartition) extends RouteToKafkaDispatcher {

  private[router] def asDispatchToKafkaMessage(requesterRef: ActorRef): DispatchFetchOffsetToKafka =
    DispatchFetchOffsetToKafka(offsetType, topicAndPartition, RequesterInfo(requesterRef))
}

object FetchOffset {
  sealed abstract class Result extends RouterResult

  case class TimedOut(request: FetchOffset, requestId: Long) extends Result with TimedOutRequest[FetchOffset]

  sealed abstract class Response extends Result {
    val offsetType: OffsetType
  }

  case class Success(offsetType: OffsetType, topicAndPartition: TopicAndPartition, offset: Long) extends Response
  case class UnknownTopicOrPartition(offsetType: OffsetType, topicAndPartition: TopicAndPartition) extends Response
}

/**
 * Send this message to RouterActor to fetch an offset for a given consumer id.
 * If successful, the sender will likely be sent a message that is a subtype of FetchConsumerOffset.Response.
 * There is no guarantee that any response will be sent.  Mix in RouterRequestManager into the client Actor
 * to handle the lack of this guarantee.
 */
case class FetchConsumerOffset(consumerId: ConsumerId, topicAndPartition: TopicAndPartition)
  extends RouteToKafkaDispatcher {

  private[router] def asDispatchToKafkaMessage(requesterRef: ActorRef): DispatchFetchConsumerOffsetToKafka =
    DispatchFetchConsumerOffsetToKafka(consumerId, topicAndPartition, RequesterInfo(requesterRef))
}

object FetchConsumerOffset {
  sealed abstract class Result extends RouterResult

  case class TimedOut(request: FetchConsumerOffset, requestId: Long) extends Result with TimedOutRequest[FetchConsumerOffset]

  sealed abstract class Response extends Result {
    val consumerId: ConsumerId
  }

  case class Success(consumerId: ConsumerId, topicAndPartition: TopicAndPartition, offsetAndMetadata: OffsetAndMetadata) extends Response
  case class NotFound(consumerId: ConsumerId, topicAndPartition: TopicAndPartition) extends Response
}

/**
 * Send this message to RouterActor to commit an offset for a given consumer id.
 * If successful, the sender will likely be sent a message that is a subtype of CommitConsumerOffset.Response.
 * There is no guarantee that any response will be sent.  Mix in RouterRequestManager into the client Actor
 * to handle the lack of this guarantee.
 */
case class CommitConsumerOffset(consumerId: ConsumerId, topicAndPartition: TopicAndPartition, offsetAndMetadata: OffsetAndMetadata)
  extends RouteToKafkaDispatcher {

  private[router] def asDispatchToKafkaMessage(requesterRef: ActorRef): DispatchCommitConsumerOffsetToKafka =
    DispatchCommitConsumerOffsetToKafka(consumerId, topicAndPartition, offsetAndMetadata, RequesterInfo(requesterRef))
}

object CommitConsumerOffset {
  sealed abstract class Result extends RouterResult

  case class TimedOut(request: CommitConsumerOffset, requestId: Long) extends Result with TimedOutRequest[CommitConsumerOffset]

  sealed abstract class Response extends Result {
    val consumerId: ConsumerId
    val offsetAndMetadata: OffsetAndMetadata
  }

  case class Success(consumerId: ConsumerId, topicAndPartition: TopicAndPartition, offsetAndMetadata: OffsetAndMetadata) extends Response
  case class UnknownTopicOrPartition(consumerId: ConsumerId, topicAndPartition: TopicAndPartition, offsetAndMetadata: OffsetAndMetadata) extends Response
  case class Superseded(consumerId: ConsumerId, topicAndPartition: TopicAndPartition,
                        offsetAndMetadata: OffsetAndMetadata, supersededBy: OffsetAndMetadata) extends Response {
    require(offsetAndMetadata.offset <= supersededBy.offset)
  }
}

/**
 * Send this message to RouterActor to fetch metadata for all topics.
 * If successful, the sender will likely be sent a message that is a subtype of FetchTopicMetadata.Response.
 * There is no guarantee that any response will be sent.  Mix in RouterRequestManager into the client Actor
 * to handle the lack of this guarantee.
 */
case class FetchTopicMetadata(requestId: Long) extends RouteToKafkaDispatcher {

  private[router] def asDispatchToKafkaMessage(requesterRef: ActorRef): DispatchFetchTopicMetadataToKafka =
    DispatchFetchTopicMetadataToKafka(requestId, RequesterInfo(requesterRef))
}

case object FetchTopicMetadata {
  sealed abstract class Result extends RouterResult

  case class TimedOut(request: FetchTopicMetadata, requestId: Long) extends Result with TimedOutRequest[FetchTopicMetadata]

  sealed abstract class Response extends Result {
    val requestId: Long
  }

  case class Success(requestId: Long, topicMetadata: Map[TopicAndPartition, CastlePartitionMetadata]) extends Response
}

private[router] sealed abstract class InternalRouterMessage extends RouterMessage

private[router] case class UnexpectedFailure(t: Throwable) extends InternalRouterMessage

private[router] case class RefreshBrokersAndLeaders[T <: DispatchToKafka](requests: Iterable[T])
  extends InternalRouterMessage

private[router] case class BrokerUnreachable(broker: Broker) extends InternalRouterMessage
