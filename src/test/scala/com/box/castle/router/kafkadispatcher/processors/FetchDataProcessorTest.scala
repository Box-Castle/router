package com.box.castle.router.kafkadispatcher.processors


import java.util.concurrent.TimeUnit

import akka.testkit.TestProbe
import com.box.castle.batch.CastleMessageBatch
import com.box.castle.router.RouterConfig
import com.box.castle.router.kafkadispatcher.KafkaDispatcherRef
import com.box.castle.router.kafkadispatcher.messages.{DispatchFetchDataToKafka, FetchDataKafkaResponse}
import com.box.castle.router.messages.FetchData
import com.box.castle.router.metrics.Metrics
import com.box.castle.router.mock.{MockMetricsLogger, MockBatchTools, MockActorTools}
import com.box.castle.consumer.CastleSimpleConsumer
import kafka.api.{FetchResponse, FetchResponsePartitionData}
import kafka.common.{ErrorMapping, TopicAndPartition}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.concurrent.duration.FiniteDuration

/**
 * Created by dgrenader on 8/11/15.
 */
class FetchDataProcessorTest extends Specification with Mockito with MockActorTools with MockBatchTools {

  val CacheMaxSizeInBytes: Int = 10000000
  val DefaultMessageSize: Int = 498452

  "FetchDataProcessor" should {
    "handle outstanding messages in the queue when getting data" in new actorSystem {
      val kafkaDispatcher = mock[KafkaDispatcherRef]
      val consumer = mock[CastleSimpleConsumer]
      val metricsLogger = new MockMetricsLogger()

      val fdp = new FetchDataProcessor(kafkaDispatcher, consumer, CacheMaxSizeInBytes, RouterConfig.DefaultConfig, metricsLogger)

      val requestedOffset: Int = 4000
      val numMessagesInResponse = 20

      // This will be our "in flight" request and we will not add it to the request queue to simulate the fetch
      // data process having already sent this out on the wire
      val topicAndPartitionOriginal = TopicAndPartition("Performance", 16)
      val mockRequesterActorRefOriginal = TestProbe()
      val requesterInfoOriginal = RequesterInfo(mockRequesterActorRefOriginal.ref)


      // We will simulate entering requests into the queue while the original request for this very topic, partition and offset
      // is already in flight to Kafka
      val topicAndPartitionOne = TopicAndPartition("Performance", 16)
      val mockRequesterActorRefOne = TestProbe()
      val requesterInfoOne = RequesterInfo(mockRequesterActorRefOne.ref)
      val dispatchOne = DispatchFetchDataToKafka(topicAndPartitionOne, requestedOffset, requesterInfoOne)
      fdp.addToQueue(dispatchOne)
      fdp.getFromCache(dispatchOne) must_== None

      val topicAndPartitionTwo = TopicAndPartition("Performance", 16)
      val mockRequesterActorRefTwo = TestProbe()
      val requesterInfoTwo = RequesterInfo(mockRequesterActorRefTwo.ref)
      val requestedOffsetTwo = requestedOffset + (numMessagesInResponse / 2)
      val dispatchTwo = DispatchFetchDataToKafka(topicAndPartitionTwo, requestedOffsetTwo , requesterInfoTwo)
      fdp.addToQueue(dispatchTwo)
      fdp.getFromCache(dispatchTwo) must_== None

      // This request is for the same topic and partition, but the offset will be too far out from what is returned in the response
      val topicAndPartitionWithNonMatchinOffset= TopicAndPartition("Performance", 16)
      val mockRequesterActorRefWithNonMatchingOffset = TestProbe()
      val requesterInfoWithNonMatchinOffset = RequesterInfo(mockRequesterActorRefWithNonMatchingOffset.ref)
      val dispatchThree = DispatchFetchDataToKafka(topicAndPartitionWithNonMatchinOffset,
        requestedOffset + (numMessagesInResponse * 2), requesterInfoWithNonMatchinOffset)
      fdp.addToQueue(dispatchThree)
      fdp.getFromCache(dispatchThree) must_== None

      val requests = Map(topicAndPartitionOriginal -> ((requestedOffset.toLong, Set(requesterInfoOriginal))))

      val mockFetchResponsePartitionData = mock[FetchResponsePartitionData]
      mockFetchResponsePartitionData.error returns ErrorMapping.NoError
      mockFetchResponsePartitionData.messages returns makeMockMessageSet(requestedOffset, numMessagesInResponse, DefaultMessageSize)

      val fetchResponse = mock[FetchResponse]
      fetchResponse.data returns Map(topicAndPartitionOriginal -> mockFetchResponsePartitionData)

      val castleResponse = FetchDataKafkaResponse(fetchResponse, requests)

      fdp.processResponse(castleResponse)

      val castleMessageBatch = CastleMessageBatch(mockFetchResponsePartitionData.messages)
      val responseToActorOne = FetchData.Success(topicAndPartitionOne, requestedOffset, castleMessageBatch)

      // both the original requester and the first actor get back the same response
      mockRequesterActorRefOriginal.expectMsg(responseToActorOne)
      mockRequesterActorRefOne.expectMsg(responseToActorOne)

      // the second actor gets a slice of the response because its offset is in the middle of the returned batch
      val castleMessageBatchTwo = castleMessageBatch.createBatchFromOffset(requestedOffsetTwo).get
      val responseToActorTwo = FetchData.Success(topicAndPartitionOne, requestedOffsetTwo, castleMessageBatchTwo)
      mockRequesterActorRefTwo.expectMsg(responseToActorTwo)

      mockRequesterActorRefWithNonMatchingOffset.expectNoMsg(FiniteDuration(500, TimeUnit.MILLISECONDS))

      metricsLogger.getCountFor(Metrics.AvoidedFetches) must_== 2
      
      // Now make sure they were removed from the queue by processing the exact same message again
      fdp.processResponse(castleResponse)

      // The original requester will still get the message sent to it
      mockRequesterActorRefOriginal.expectMsgAllOf(FiniteDuration(500, TimeUnit.MILLISECONDS), responseToActorOne)

      // but the ones in the queue will not get anything
      mockRequesterActorRefOne.expectNoMsg(FiniteDuration(500, TimeUnit.MILLISECONDS))
      mockRequesterActorRefTwo.expectNoMsg(FiniteDuration(500, TimeUnit.MILLISECONDS))
      mockRequesterActorRefWithNonMatchingOffset.expectNoMsg(FiniteDuration(500, TimeUnit.MILLISECONDS))

      metricsLogger.getCountFor(Metrics.AvoidedFetches) must_== 2

      // Cache should now be able to satisfy request one and two
      fdp.getFromCache(dispatchOne) must_== Some(responseToActorOne)
      fdp.getFromCache(dispatchTwo) must_== Some(responseToActorTwo)
    }

    "handle outstanding messages in the queue when getting NO DATA" in new actorSystem {
      val kafkaDispatcher = mock[KafkaDispatcherRef]
      val consumer = mock[CastleSimpleConsumer]
      val metricsLogger = new MockMetricsLogger()

      val fdp = new FetchDataProcessor(kafkaDispatcher, consumer, CacheMaxSizeInBytes, RouterConfig.DefaultConfig, metricsLogger)

      val requestedOffset: Int = 4000
      val numMessagesInResponse = 0

      // This will be our "in flight" request and we will not add it to the request queue to simulate the fetch
      // data process having already sent this out on the wire
      val topicAndPartitionOriginal = TopicAndPartition("Performance", 16)
      val mockRequesterActorRefOriginal = TestProbe()
      val requesterInfoOriginal = RequesterInfo(mockRequesterActorRefOriginal.ref)


      // We will simulate entering requests into the queue while the original request for this very topic, partition and offset
      // is already in flight to Kafka
      val topicAndPartitionOne = TopicAndPartition("Performance", 16)
      val mockRequesterActorRefOne = TestProbe()
      val requesterInfoOne = RequesterInfo(mockRequesterActorRefOne.ref)
      fdp.addToQueue(DispatchFetchDataToKafka(topicAndPartitionOne, requestedOffset, requesterInfoOne))

      val topicAndPartitionTwo = TopicAndPartition("Performance", 16)
      val mockRequesterActorRefTwo = TestProbe()
      val requesterInfoTwo = RequesterInfo(mockRequesterActorRefTwo.ref)
      val requestedOffsetTwo = requestedOffset + 15
      fdp.addToQueue(DispatchFetchDataToKafka(topicAndPartitionTwo, requestedOffsetTwo , requesterInfoTwo))

      // This request is for the same topic and partition, but the offset will be too far out from what is returned in the response
      val topicAndPartitionWithNonMatchinOffset= TopicAndPartition("Performance", 16)
      val mockRequesterActorRefWithNonMatchingOffset = TestProbe()
      val requesterInfoWithNonMatchinOffset = RequesterInfo(mockRequesterActorRefWithNonMatchingOffset.ref)
      fdp.addToQueue(DispatchFetchDataToKafka(topicAndPartitionWithNonMatchinOffset,
        requestedOffset + 200, requesterInfoWithNonMatchinOffset))

      val requests = Map(topicAndPartitionOriginal -> ((requestedOffset.toLong, Set(requesterInfoOriginal))))

      val mockFetchResponsePartitionData = mock[FetchResponsePartitionData]
      mockFetchResponsePartitionData.error returns ErrorMapping.NoError
      mockFetchResponsePartitionData.messages returns makeMockMessageSet(requestedOffset, numMessagesInResponse, DefaultMessageSize)

      val fetchResponse = mock[FetchResponse]
      fetchResponse.data returns Map(topicAndPartitionOriginal -> mockFetchResponsePartitionData)

      val castleResponse = FetchDataKafkaResponse(fetchResponse, requests)

      fdp.processResponse(castleResponse)

      val responseToActorOne = FetchData.NoMessages(topicAndPartitionOne, requestedOffset)

      // both the original requester and the first actor get back the same response
      mockRequesterActorRefOriginal.expectMsg(responseToActorOne)
      mockRequesterActorRefOne.expectMsg(responseToActorOne)

      // the second actor should not get anything since it's offset request is different
      mockRequesterActorRefTwo.expectNoMsg(FiniteDuration(500, TimeUnit.MILLISECONDS))

      mockRequesterActorRefWithNonMatchingOffset.expectNoMsg(FiniteDuration(500, TimeUnit.MILLISECONDS))

      metricsLogger.getCountFor(Metrics.AvoidedFetches) must_== 1

      // Now make sure they were removed from the queue by processing the exact same message again
      fdp.processResponse(castleResponse)

      // The original requester will still get the message sent to it
      mockRequesterActorRefOriginal.expectMsgAllOf(FiniteDuration(500, TimeUnit.MILLISECONDS), responseToActorOne)

      // but the ones in the queue will not get anything
      mockRequesterActorRefOne.expectNoMsg(FiniteDuration(500, TimeUnit.MILLISECONDS))
      mockRequesterActorRefTwo.expectNoMsg(FiniteDuration(500, TimeUnit.MILLISECONDS))
      mockRequesterActorRefWithNonMatchingOffset.expectNoMsg(FiniteDuration(500, TimeUnit.MILLISECONDS))

      metricsLogger.getCountFor(Metrics.AvoidedFetches) must_== 1
    }
  }
}
