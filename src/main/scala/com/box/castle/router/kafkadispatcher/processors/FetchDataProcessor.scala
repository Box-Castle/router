package com.box.castle.router.kafkadispatcher.processors

import com.box.castle.batch.CastleMessageBatch
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.RouterConfig
import com.box.castle.router.kafkadispatcher.cache.FetchDataProcessorCache
import com.box.castle.router.kafkadispatcher.messages.{DispatchFetchDataToKafka, FetchDataKafkaResponse, LeaderNotAvailable}
import com.box.castle.router.kafkadispatcher.{KafkaDispatcherRef, RequestQueue}
import com.box.castle.router.messages.{RefreshBrokersAndLeaders, FetchData}
import com.box.castle.consumer.CastleSimpleConsumer
import com.box.castle.router.metrics.{TagNames, Metrics, Components}
import org.slf4s.Logging
import kafka.api.{FetchResponse, FetchResponsePartitionData}
import kafka.common.TopicAndPartition
import kafka.message.MessageSet

import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext




private[kafkadispatcher]
class FetchDataProcessor(kafkaDispatcher: KafkaDispatcherRef,
                         consumer: CastleSimpleConsumer,
                         cacheMaxSizeInBytes: Long,
                         routerConfig: RouterConfig,
                         metricsLogger: MetricsLogger)
  extends QueueProcessor[DispatchFetchDataToKafka, FetchDataKafkaResponse](
    kafkaDispatcher, consumer, metricsLogger) with Logging {

  require(cacheMaxSizeInBytes > 0, "Max cache size must be greater than 0")

  private val byteFormatter = java.text.NumberFormat.getIntegerInstance

  private var requestQueue = RequestQueue.empty[TopicAndPartition, Long]
  private var cache = FetchDataProcessorCache(cacheMaxSizeInBytes)
  private var correlationId = 0

  override def getFromCache(request: DispatchFetchDataToKafka): Option[FetchData.Success] = {
    val topicAndPartition = request.topicAndPartition
    val offset = request.offset
    cache.get(topicAndPartition, offset).map(castleMessageBatch => {
      count(Metrics.CacheHits)
      FetchData.Success(topicAndPartition, offset, castleMessageBatch)
    })
  }

  def addToQueue(request: DispatchFetchDataToKafka): Unit = {
    requestQueue = requestQueue.add(request.topicAndPartition, request.offset, request.requesterInfo)
  }

  /**
   * Returns true if the message fetcher has outstanding requests in the queue
   * @return
   */
  def isEmpty = requestQueue.isEmpty

  /**
   * Issues a network call to the Kafka broker to fetch the offsets specified in our request queue.
   * This call will not drain the entire request queue if there are multiple offset requests for the
   * same topic partition combination.
   */
  def processQueue()(implicit ec: ExecutionContext): Unit = {
    val (requests: Map[TopicAndPartition, (Long, Set[RequesterInfo])], remainingRequestQueue) = requestQueue.removeHead()
    requestQueue = remainingRequestQueue
    count(Metrics.NumFetches)
    correlationId += 1
    async({
      val start = System.nanoTime()
      val result = consumer.fetchData(requests.map { case (topicAndPartition, (offset, _)) => (topicAndPartition, offset)},
        correlationId, maxWaitTime = routerConfig.maxWaitTime, minBytes = routerConfig.minBytes)
      val total = (System.nanoTime() - start) / 1000000
      log.info(s"Data fetch request $correlationId to $consumer got ${byteFormatter.format(result.sizeInBytes)} " +
        s"bytes for ${requests.size} topicAndPartitions in $total ms")
      result
    },
    onSuccess = (response: FetchResponse) => kafkaDispatcher ! FetchDataKafkaResponse(response, requests))
  }

  /**
   * Handles the response from the broker
   */
  def processResponse(kafkaResponse: FetchDataKafkaResponse)(implicit ec: ExecutionContext): Unit = {
    val response = kafkaResponse.response

    val requests: Map[TopicAndPartition, (Long, Set[RequesterInfo])] = kafkaResponse.requests
    require(response.data.size == requests.size)
    require(response.correlationId == correlationId)

    count(Metrics.BytesFetched, response.sizeInBytes)
    response.data.foreach {
      case (topicAndPartition, partitionData: FetchResponsePartitionData) => {
        assert(requests.contains(topicAndPartition))

        val (offset, requesters) = requests(topicAndPartition)

        checkErrorCode(partitionData.error,
          noError = {
            processMessages(partitionData.messages, topicAndPartition, requests)
          },
          unexpectedServerError = {
            log.error(s"FetchDataProcessor got an 'UnknownError' code back " +
              s"from $consumer for: $topicAndPartition, offset: $offset, retrying the request...")
            // We will retry these
            requesters.foreach(r => addToQueue(DispatchFetchDataToKafka(topicAndPartition, offset, r)))

          },
          offsetOutOfRange = {
            requesters.foreach(requestInfo =>
              requestInfo.ref ! FetchData.OffsetOutOfRange(topicAndPartition, offset))
          },
          unknownTopicOrPartitionCode = {
            handleUnknownTopicOrPartitionCode(topicAndPartition)
            requesters.foreach(requestInfo =>
              requestInfo.ref ! FetchData.UnknownTopicOrPartition(topicAndPartition, offset))
          },
          leaderNotAvailable = {
            handleLeaderNotAvailable(offset, requesters, topicAndPartition)
          },
          invalidMessageCode = {
            log.error(s"FetchDataProcessor encountered an InvalidMessageCode while " +
              s"fetching offset: $offset for $topicAndPartition from ${consumer.brokerInfo}")
            count(Metrics.InvalidMessages)
            processMessages(partitionData.messages, topicAndPartition, requests)
          }
        )
      }
    }
  }

  private def handleLeaderNotAvailable(offset: Long,
                                       requesters: Set[RequesterInfo],
                                       topicAndPartition: TopicAndPartition): Unit = {
    val internalRequests =
      requesters.foldLeft(Queue.empty[DispatchFetchDataToKafka])((ir, requesterInfo) =>
        ir.enqueue(DispatchFetchDataToKafka(topicAndPartition, offset, requesterInfo)))

    val finalInternalRequests =
      requestQueue.get(topicAndPartition) match {
        case Some(offsetQueue) => {
          offsetQueue.foldLeft(internalRequests)((requests, offsetAndRequesters) =>
            offsetAndRequesters._2.foldLeft(requests)((innerRequests, requester) =>
              innerRequests.enqueue(DispatchFetchDataToKafka(topicAndPartition, offsetAndRequesters._1, requester))))
        }
        case None => internalRequests
      }

    // Remove all requests for this topic and partition from the request queue
    requestQueue = requestQueue.remove(topicAndPartition)

    kafkaDispatcher ! LeaderNotAvailable(finalInternalRequests)
  }

  private def processMessages(messages: MessageSet,
                  topicAndPartition: TopicAndPartition,
                  requests: Map[TopicAndPartition, (Long, Set[RequesterInfo])]): Unit = {
    val (offset, requesters) = requests(topicAndPartition)
    count(Metrics.BatchedRequests, requesters.size)
    if (messages.nonEmpty) {
      try {
        val castleMessageBatch = CastleMessageBatch(messages)
        if (castleMessageBatch.offset != offset) {
          throw new RuntimeException(s"Broker $consumer returned a message set with an " +
            s"offset of ${castleMessageBatch.offset} which does not match the requested offset of $offset")
        }
        cache = cache.add(topicAndPartition, castleMessageBatch)

        requesters.foreach((requesterInfo: RequesterInfo) =>
          requesterInfo.ref ! FetchData.Success(topicAndPartition, offset, castleMessageBatch))

        checkRequestQueue(topicAndPartition, castleMessageBatch)
      }
      catch {
        case ex: kafka.message.InvalidMessageException => {
          log.warn(s"Broker $consumer encountered a corrupt message for $topicAndPartition at offset $offset", ex)
          requesters.foreach((requesterInfo: RequesterInfo) =>
            requesterInfo.ref ! FetchData.CorruptMessage(topicAndPartition, offset, offset + 1))
        }
      }
    }
    else {
      requesters.foreach(requesterInfo => requesterInfo.ref ! FetchData.NoMessages(topicAndPartition, offset))

      // There might be requests in the request queue outstanding for this specific offset
      requestQueue.get(topicAndPartition).foreach(offsetQueue => {
        offsetQueue.foreach({
          case (rqOffset, rqRequesters) => {
            if (offset == rqOffset) {
              count(Metrics.AvoidedFetches, rqRequesters.size)
              rqRequesters.foreach(requesterInfo => requesterInfo.ref ! FetchData.NoMessages(topicAndPartition, offset))
              requestQueue = requestQueue.remove(topicAndPartition, offset)
            }
          }
        })
      })
    }
  }

  private def checkRequestQueue(topicAndPartition: TopicAndPartition, castleMessageBatch: CastleMessageBatch): Unit = {
    requestQueue.get(topicAndPartition).foreach(offsetQueue => {
      // We go through each offset in the offset queue for this topic and partition
      offsetQueue.foreach({
        case (offset, requesters) => {
          // We attempt to create a matching CastleMessageBatch from the CastleMessageBatch we got from Kafka
          // by using this particular offset.  What this does is that it ensures if there's a request
          // in the offset queue for an offset that is already contained in this CastleMessageBatch that
          // we will not go to the server to get it, we simply take a slice of the CastleMessageBatch we already
          // got and use that instead

          castleMessageBatch.createBatchFromOffset(offset).foreach(castleMessageBatchForOffset => {
            // Here we have successfully gotten a CastleMessageBatch from the one we got from the server
            count(Metrics.AvoidedFetches, requesters.size)
            requesters.foreach(requesterInfo =>
              requesterInfo.ref ! FetchData.Success(topicAndPartition, offset, castleMessageBatchForOffset))
            requestQueue = requestQueue.remove(topicAndPartition, offset)
          })
        }
      })
    })
  }

  def setCacheSize(cacheSizeInBytes: Long): Unit = {
    log.info(s"Setting max cache size to ${byteFormatter.format(cacheSizeInBytes)} bytes for $consumer")
    cache = cache.setMaxSizeInBytes(cacheSizeInBytes)
  }

}