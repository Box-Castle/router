package com.box.castle.router.kafkadispatcher.processors

import com.box.castle.router.kafkadispatcher._
import com.box.castle.router.kafkadispatcher.messages._
import com.box.castle.router.messages.{OffsetAndMetadata, FetchOffset}
import com.box.castle.consumer.{CastleSimpleConsumer, OffsetType}
import org.slf4s.Logging
import kafka.api._
import kafka.common.TopicAndPartition

import scala.concurrent.ExecutionContext




private[kafkadispatcher]
class FetchOffsetProcessor(kafkaDispatcher: KafkaDispatcherRef,
                           consumer: CastleSimpleConsumer)
  extends QueueProcessor[DispatchFetchOffsetToKafka, FetchOffsetKafkaResponse](kafkaDispatcher) with Logging {

  // We always fetch just one offset
  private val MaxNumOffsets = 1

  private var correlationId = 0
  private var requestQueue = RequestQueue.empty[TopicAndPartition, OffsetType]

  def addToQueue(request: DispatchFetchOffsetToKafka) = {
    requestQueue = requestQueue.add(request.topicAndPartition, request.offsetType, request.requesterInfo)
  }

  def processResponse(castleResponse: FetchOffsetKafkaResponse)(implicit ec: ExecutionContext) = {
    val response = castleResponse.response
    val requests = castleResponse.requests

    assert(response.partitionErrorAndOffsets.size == requests.size)
    assert(response.correlationId == correlationId)

    response.partitionErrorAndOffsets.foreach {
      case (topicAndPartition, partitionOffsetResponse) => {
        val (offsetType, requesters) = requests(topicAndPartition)

        checkErrorCode(partitionOffsetResponse.error,
          noError = {
            success(topicAndPartition, partitionOffsetResponse, offsetType, requesters)
          },
          unexpectedServerError = {
            log.error(s"OffsetFetchProcessor got an 'UnknownError' code back " +
              s"from $consumer for: $topicAndPartition, offsetType: $offsetType, retrying the request...")
            // We will retry these
            requesters.foreach(r => addToQueue(DispatchFetchOffsetToKafka(offsetType, topicAndPartition, r)))
          },
          unknownTopicOrPartitionCode = {
            requesters.foreach(requesterInfo =>
              requesterInfo.ref ! FetchOffset.UnknownTopicOrPartition(offsetType, topicAndPartition))
          },
          leaderNotAvailable = {
            val internalOffsetFetchRequests = requesters.map(requestInfo =>
              DispatchFetchOffsetToKafka(offsetType,topicAndPartition, requestInfo))

            // Remove all requests for this topic and partition from the request queue
            requestQueue = requestQueue.remove(topicAndPartition)

            kafkaDispatcher ! LeaderNotAvailable(internalOffsetFetchRequests)
          },
          invalidMessageCode = {
            log.error(s"FetchOffsetProcessor encountered an InvalidMessageCode while " +
              s"fetching $offsetType offset for $topicAndPartition from ${consumer.brokerInfo}")
            success(topicAndPartition, partitionOffsetResponse, offsetType, requesters)
          }
        )
      }
    }
  }

  private def success(topicAndPartition: TopicAndPartition,
              partitionOffsetResponse: PartitionOffsetsResponse,
              offsetType: OffsetType,
              requesters: Set[RequesterInfo]): Unit = {
    assert(partitionOffsetResponse.offsets.size == MaxNumOffsets)
    requesters.foreach(requesterInfo =>
    requesterInfo.ref ! FetchOffset.Success(offsetType,
    topicAndPartition,
    partitionOffsetResponse.offsets.head))
  }

  def isEmpty = requestQueue.isEmpty

  def processQueue()(implicit ec: ExecutionContext): Unit = {
    val (requests: Map[TopicAndPartition, (OffsetType, Set[RequesterInfo])], newRequestQueue) = requestQueue.removeHead()
    requestQueue = newRequestQueue

    correlationId += 1

    async({
      consumer.fetchOffsets(requests.mapValues(offsetTypeAndRequesters => offsetTypeAndRequesters._1), correlationId)
    },
    onSuccess = (response: OffsetResponse) =>
      kafkaDispatcher ! FetchOffsetKafkaResponse(response, requests)
    )
  }
}
