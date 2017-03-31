package com.box.castle.router.kafkadispatcher.processors

import com.box.castle.collections.immutable.LinkedHashMap
import com.box.castle.router.kafkadispatcher.KafkaDispatcherRef
import com.box.castle.router.kafkadispatcher.messages.{DispatchFetchConsumerOffsetToKafka, FetchConsumerOffsetKafkaResponse, LeaderNotAvailable}
import com.box.castle.router.messages.{OffsetAndMetadata, FetchConsumerOffset}
import com.box.castle.consumer.{CastleSimpleConsumer, ConsumerId}
import org.slf4s.Logging
import kafka.api.OffsetFetchResponse
import kafka.common.{OffsetMetadataAndError, TopicAndPartition}

import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext




private[kafkadispatcher]
class FetchConsumerOffsetProcessor(kafkaDispatcher: KafkaDispatcherRef,
                                   consumer: CastleSimpleConsumer)
  extends QueueProcessor[DispatchFetchConsumerOffsetToKafka, FetchConsumerOffsetKafkaResponse](kafkaDispatcher) with Logging {

  private var correlationId = 0
  private var requestQueue = LinkedHashMap.empty[ConsumerId, Map[TopicAndPartition, Set[RequesterInfo]]]
      .withDefaultValue(Map.empty[TopicAndPartition, Set[RequesterInfo]]
      .withDefaultValue(Set.empty[RequesterInfo]))

  def addToQueue(request: DispatchFetchConsumerOffsetToKafka): Unit = {
    val consumerId = request.consumerId
    val topicAndPartition = request.topicAndPartition
    val existingRequestersByTopicAndPartition = requestQueue(consumerId)
    val existingRequesters = existingRequestersByTopicAndPartition(topicAndPartition)
    val newRequestersByTopicAndPartition = existingRequestersByTopicAndPartition +
      (topicAndPartition -> (existingRequesters + request.requesterInfo))
    requestQueue = requestQueue + (consumerId -> newRequestersByTopicAndPartition)
  }

  def processResponse(castleResponse: FetchConsumerOffsetKafkaResponse)(implicit ec: ExecutionContext): Unit = {
    val consumerId = castleResponse.consumerId
    val response = castleResponse.response
    val requests = castleResponse.requests

    assert(response.requestInfo.size == requests.size)
    assert(response.correlationId == correlationId)

    response.requestInfo.foreach {
      case (topicAndPartition, offsetMetadataAndError) => {
        val requesters = requests(topicAndPartition)

        checkErrorCode(offsetMetadataAndError.error,
          noError = {
            success(requesters, consumerId, topicAndPartition, offsetMetadataAndError)
          },
          unexpectedServerError = {
            log.error(s"ConsumerOffsetFetchProcessor got an 'UnknownError' code back " +
              s"from $consumer for: $topicAndPartition, consumerId: $consumerId, retrying the request...")
            // We will retry these
            requesters.foreach(r => addToQueue(DispatchFetchConsumerOffsetToKafka(consumerId, topicAndPartition, r)))
          },
          unknownTopicOrPartitionCode = {
            requesters.foreach(requesterInfo =>
              requesterInfo.ref ! FetchConsumerOffset.NotFound(consumerId, topicAndPartition))
          },
          leaderNotAvailable = {
            handleLeaderNotAvailable(consumerId, requesters, topicAndPartition)
          },
          invalidMessageCode = {
            log.error(s"FetchConsumerOffsetProcessor encountered an InvalidMessageCode while " +
              s"fetching the offset for consumer $consumerId for $topicAndPartition from ${consumer.brokerInfo}")
            success(requesters, consumerId, topicAndPartition, offsetMetadataAndError)
          }
        )
      }
    }
  }

  private def success(requesters: Set[RequesterInfo],
                      consumerId: ConsumerId,
                      topicAndPartition: TopicAndPartition,
                      offsetMetadataAndError: OffsetMetadataAndError): Unit = {
    requesters.foreach(requesterInfo =>
      requesterInfo.ref ! FetchConsumerOffset.Success(consumerId, topicAndPartition, OffsetAndMetadata(offsetMetadataAndError)))
  }

  private def handleLeaderNotAvailable(consumerId: ConsumerId,
                                       requesters: Set[RequesterInfo],
                                       topicAndPartition: TopicAndPartition): Unit = {
    val internalRequests =
      requesters.foldLeft(Queue.empty[DispatchFetchConsumerOffsetToKafka])((ir, requesterInfo) =>
        ir.enqueue(DispatchFetchConsumerOffsetToKafka(consumerId, topicAndPartition, requesterInfo))
      )

    val finalInternalRequests = requestQueue.foldLeft(internalRequests) {
      case (ir, (rqConsumerId, topicAndPartitionToRequestersMap)) => {
        topicAndPartitionToRequestersMap.get(topicAndPartition) match {
          case Some(rqRequesters) => {
            val newTopicAndPartitionToRequestersMap = topicAndPartitionToRequestersMap - topicAndPartition
            if (newTopicAndPartitionToRequestersMap.isEmpty)
              requestQueue = requestQueue - rqConsumerId
            else
              requestQueue = requestQueue + (rqConsumerId -> newTopicAndPartitionToRequestersMap)

            rqRequesters.foldLeft(ir)((ir, requesterInfo) =>
              ir.enqueue(DispatchFetchConsumerOffsetToKafka(rqConsumerId, topicAndPartition, requesterInfo)))
          }
          case None => ir
        }
      }
    }
    kafkaDispatcher ! LeaderNotAvailable(finalInternalRequests)
  }

  def isEmpty = requestQueue.isEmpty

  def processQueue()(implicit ec: ExecutionContext): Unit = {
    val (consumerId, requests: Map[TopicAndPartition, Set[RequesterInfo]], newRequestQueue) = requestQueue.removeHead()
    requestQueue = newRequestQueue

    correlationId += 1
    async({
      consumer.fetchConsumerOffsets(consumerId, requests.keys.toSeq, correlationId)
    },
    onSuccess = (response: OffsetFetchResponse) => {
      kafkaDispatcher ! FetchConsumerOffsetKafkaResponse(consumerId, response, requests)
    })
  }
}
