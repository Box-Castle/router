package com.box.castle.router.kafkadispatcher.processors

import com.box.castle.collections.immutable.LinkedHashMap
import com.box.castle.router.exceptions.RouterFatalException
import com.box.castle.router.kafkadispatcher.KafkaDispatcherRef
import com.box.castle.router.kafkadispatcher.messages.{CommitConsumerOffsetKafkaResponse, DispatchCommitConsumerOffsetToKafka, LeaderNotAvailable}
import com.box.castle.router.messages.{OffsetAndMetadata, CommitConsumerOffset}
import com.box.castle.consumer.{CastleSimpleConsumer, ConsumerId}
import org.slf4s.Logging
import kafka.api.OffsetCommitResponse
import kafka.common.TopicAndPartition

import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext

/**
 * Created by dgrenader on 4/12/15.
 */
private[kafkadispatcher] class CommitConsumerOffsetProcessor(
    kafkaDispatcher: KafkaDispatcherRef,
    consumer: CastleSimpleConsumer)
  extends QueueProcessor[DispatchCommitConsumerOffsetToKafka, CommitConsumerOffsetKafkaResponse](kafkaDispatcher) with Logging {

  private var correlationId = 0

  var requestQueue = LinkedHashMap.empty[ConsumerId, Map[TopicAndPartition, (OffsetAndMetadata, RequesterInfo)]]
    .withDefaultValue(Map.empty[TopicAndPartition, (OffsetAndMetadata, RequesterInfo)])

  def addToQueue(request: DispatchCommitConsumerOffsetToKafka): Unit = {
    val consumerId = request.consumerId
    val topicAndPartition = request.topicAndPartition
    val offsetAndMetadata = request.offsetAndMetadata
    val requesterInfo = request.requesterInfo

    val existingOffsetsByTopicAndPartition = requestQueue(consumerId)

    existingOffsetsByTopicAndPartition.get(topicAndPartition) match {
      case Some((existingOffsetAndMetadata, existingRequester)) => {
        if (offsetAndMetadata.offset > existingOffsetAndMetadata.offset) {
          // We will only commit offsets that are newer than what is already queued up
          existingRequester ! CommitConsumerOffset.Superseded(consumerId, topicAndPartition,
            existingOffsetAndMetadata, offsetAndMetadata)

          val newOffsetsByTopicAndPartition = existingOffsetsByTopicAndPartition + (topicAndPartition -> ((offsetAndMetadata, requesterInfo)))
          requestQueue = requestQueue + (consumerId -> newOffsetsByTopicAndPartition)
        } else {
          //why do we send superseded in this case? And reversed the order of two offsets?
          requesterInfo ! CommitConsumerOffset.Superseded(consumerId, topicAndPartition,
            offsetAndMetadata, existingOffsetAndMetadata)
        }
      }
      case None => {
        val newOffsetsByTopicAndPartition = existingOffsetsByTopicAndPartition + (topicAndPartition -> ((offsetAndMetadata, requesterInfo)))
        requestQueue = requestQueue + (consumerId -> newOffsetsByTopicAndPartition)
      }
    }
  }

  def processResponse(castleResponse: CommitConsumerOffsetKafkaResponse)(implicit ec: ExecutionContext): Unit = {
    val consumerId = castleResponse.consumerId
    val response = castleResponse.response
    val requests = castleResponse.requests

    assert(response.requestInfo.size == requests.size)
    assert(response.correlationId == correlationId)

    response.requestInfo.foreach {
      case (topicAndPartition, errorCode) => {
        val (offsetAndMetadata, requester) = requests(topicAndPartition)

        checkErrorCode(errorCode,
          noError = {
            success(requester, consumerId, topicAndPartition, offsetAndMetadata)
          },
          unexpectedServerError = {
            log.error(s"ConsumerOffsetCommitProcessor got an 'UnknownError' code back " +
              s"from $consumer for: $topicAndPartition, consumerId: $consumerId, offset: ${offsetAndMetadata.offset}, retrying the request...")
            // We will retry these
            addToQueue(DispatchCommitConsumerOffsetToKafka(consumerId, topicAndPartition, offsetAndMetadata, requester))
          },
          unknownTopicOrPartitionCode = {
            requester.ref ! CommitConsumerOffset.UnknownTopicOrPartition(consumerId, topicAndPartition, offsetAndMetadata)
          },
          leaderNotAvailable = {
            handleLeaderNotAvailable(consumerId, offsetAndMetadata, requester, topicAndPartition)
          },
          offsetMetadataTooLarge = {
            throw new RouterFatalException("ConsumerOffsetCommitProcessor only commits the numeric offset, not" +
              " arbitrary data that can exceed the max size")
          },
          invalidMessageCode = {
            log.error(s"CommitConsumerOffsetProcessor encountered an InvalidMessageCode while " +
              s"committing offset ${offsetAndMetadata.offset} for consumer $consumerId for $topicAndPartition from ${consumer.brokerInfo}")
            success(requester, consumerId, topicAndPartition, offsetAndMetadata)
          }
        )
      }
    }
  }

  private def success(requester: RequesterInfo,
                      consumerId: ConsumerId,
                      topicAndPartition: TopicAndPartition,
                      offsetAndMetadata: OffsetAndMetadata): Unit = {
    requester.ref ! CommitConsumerOffset.Success(consumerId, topicAndPartition, offsetAndMetadata)
  }

  def handleLeaderNotAvailable(consumerId: ConsumerId,
                                       offsetAndMetadata: OffsetAndMetadata,
                                       requester: RequesterInfo,
                                       topicAndPartition: TopicAndPartition): Unit = {

    val internalRequests = Queue(
      DispatchCommitConsumerOffsetToKafka(consumerId, topicAndPartition, offsetAndMetadata, requester))

    val finalInternalRequests = requestQueue.foldLeft(internalRequests) {
      case (ir, (rqConsumerId, topicAndPartitionToOffsetAndRequesterMap)) => {
        topicAndPartitionToOffsetAndRequesterMap.get(topicAndPartition) match {
          case Some((rqOffsetAndMetadata, rqRequester)) => {
            val newTopicAndPartitionToOffsetAndRequesterMap = topicAndPartitionToOffsetAndRequesterMap - topicAndPartition
            if (newTopicAndPartitionToOffsetAndRequesterMap.isEmpty)
              requestQueue = requestQueue - rqConsumerId
            else
              requestQueue = requestQueue + (rqConsumerId -> newTopicAndPartitionToOffsetAndRequesterMap)

            ir.enqueue(DispatchCommitConsumerOffsetToKafka(rqConsumerId, topicAndPartition, rqOffsetAndMetadata, rqRequester))
          }
          case None => ir
        }
      }
    }
    kafkaDispatcher ! LeaderNotAvailable(finalInternalRequests)
  }

  def isEmpty = requestQueue.isEmpty

  def processQueue()(implicit ec: ExecutionContext): Unit = {
    val (consumerId, requests, newConsumerOffsetCommitQueue) = requestQueue.removeHead()
    requestQueue = newConsumerOffsetCommitQueue

    correlationId += 1
    async({
      consumer.commitConsumerOffsetAndMetadata(consumerId, requests.map {
        case (topicAndPartition, offsetAndRequests) =>
          topicAndPartition -> offsetAndRequests._1.asKafkaOffsetMetadataAndError
      }, correlationId)
    },
    onSuccess = (response: OffsetCommitResponse) => {
      kafkaDispatcher ! CommitConsumerOffsetKafkaResponse(consumerId, response, requests)
    })
  }
}
