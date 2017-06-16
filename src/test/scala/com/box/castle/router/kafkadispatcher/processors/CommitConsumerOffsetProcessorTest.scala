package com.box.castle.router.kafkadispatcher.processors

import akka.testkit.TestProbe
import com.box.castle.collections.immutable.LinkedHashMap
import com.box.castle.router.kafkadispatcher.KafkaDispatcherRef
import com.box.castle.router.kafkadispatcher.messages.{CommitConsumerOffsetKafkaResponse, LeaderNotAvailable, DispatchCommitConsumerOffsetToKafka}
import com.box.castle.router.messages.{RefreshBrokersAndLeaders, OffsetAndMetadata, CommitConsumerOffset}
import com.box.castle.consumer.{ConsumerId, CastleSimpleConsumer}
import com.box.castle.router.mock.{MockActorTools, MockMetricsLogger}
import kafka.api.OffsetCommitResponse
import kafka.common.{ErrorMapping, TopicAndPartition}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.collection.immutable.Queue




class CommitConsumerOffsetProcessorTest extends Specification with Mockito with MockActorTools {

  val CacheMaxSizeInBytes: Int = 10000000
  val DefaultMessageSize: Int = 498452

  val consumerId = ConsumerId("mock_id")
  val consumerId2 = ConsumerId("mock_id_2")
  val topicAndPartition = TopicAndPartition("perf", 17)
  val topicAndPartition2 = TopicAndPartition("api", 4)

  "CommitConsumerOffsetProcessor" should {
    "handle stale offsets correctly when adding to queue" in {
      val kafkaDispatcher = mock[KafkaDispatcherRef]
      val metricsLogger = new MockMetricsLogger()
      val processor = new CommitConsumerOffsetProcessor(kafkaDispatcher, mock[CastleSimpleConsumer], metricsLogger)

      // Initial add is super easy
      val requesterInfo = mock[RequesterInfo]
      processor.addToQueue(DispatchCommitConsumerOffsetToKafka(consumerId, topicAndPartition, OffsetAndMetadata(100, None), requesterInfo))
      processor.requestQueue must_== Map(consumerId -> Map(topicAndPartition -> ((OffsetAndMetadata(100, None), requesterInfo))))

      // Adding 200 supersedes the old 100
      processor.addToQueue(DispatchCommitConsumerOffsetToKafka(consumerId, topicAndPartition, OffsetAndMetadata(200, None), requesterInfo))
      processor.requestQueue must_== Map(consumerId -> Map(topicAndPartition -> ((OffsetAndMetadata(200, None), requesterInfo))))
      def assert1() =
        there was one(requesterInfo).!(CommitConsumerOffset.Superseded(consumerId, topicAndPartition, OffsetAndMetadata(100, None), OffsetAndMetadata(200, None)))
      assert1()

      // Adding 17 fails as it is itself superseded by the existing 200
      processor.addToQueue(DispatchCommitConsumerOffsetToKafka(consumerId, topicAndPartition, OffsetAndMetadata(17, None), requesterInfo))
      processor.requestQueue must_== Map(consumerId -> Map(topicAndPartition -> ((OffsetAndMetadata(200, None), requesterInfo))))
      def assert2() =
        there was one(requesterInfo).!(CommitConsumerOffset.Superseded(consumerId, topicAndPartition, OffsetAndMetadata(17, None), OffsetAndMetadata(200, None)))
      assert2()

      // A different requester with a lower offset still fails
      val requesterInfo2 = mock[RequesterInfo]
      processor.addToQueue(DispatchCommitConsumerOffsetToKafka(consumerId, topicAndPartition, OffsetAndMetadata(23, None), requesterInfo2))
      processor.requestQueue must_== Map(consumerId -> Map(topicAndPartition -> ((OffsetAndMetadata(200, None), requesterInfo))))
      def assert3() =
        there was one(requesterInfo2).!(CommitConsumerOffset.Superseded(consumerId, topicAndPartition, OffsetAndMetadata(23, None), OffsetAndMetadata(200, None)))
      assert3()

      // A different requester with a higher offset succeeds
      processor.addToQueue(DispatchCommitConsumerOffsetToKafka(consumerId, topicAndPartition, OffsetAndMetadata(309, None), requesterInfo2))
      processor.requestQueue must_== Map(consumerId -> Map(topicAndPartition -> ((OffsetAndMetadata(309, None), requesterInfo2))))
      def assert4() =
        there was one(requesterInfo).!(CommitConsumerOffset.Superseded(consumerId, topicAndPartition, OffsetAndMetadata(200, None), OffsetAndMetadata(309, None)))
      assert4()

      // Adding a different topic and partition for the same consumerId is acceptable even with low offsets as it
      // is completely independent
      processor.addToQueue(DispatchCommitConsumerOffsetToKafka(consumerId, topicAndPartition2, OffsetAndMetadata(11, None), requesterInfo2))
      processor.requestQueue must_== Map(consumerId -> Map(topicAndPartition -> ((OffsetAndMetadata(309, None), requesterInfo2)),
                                                           topicAndPartition2 -> ((OffsetAndMetadata(11, None), requesterInfo2))))

      // Superseding works correctly for the second topic and partition as well
      processor.addToQueue(DispatchCommitConsumerOffsetToKafka(consumerId, topicAndPartition2, OffsetAndMetadata(43, None), requesterInfo2))
      processor.requestQueue must_== Map(consumerId -> Map(topicAndPartition -> ((OffsetAndMetadata(309, None), requesterInfo2)),
                                                           topicAndPartition2 -> ((OffsetAndMetadata(43, None), requesterInfo2))))
      def assert5() =
        there was one(requesterInfo2).!(CommitConsumerOffset.Superseded(consumerId, topicAndPartition2, OffsetAndMetadata(11, None), OffsetAndMetadata(43, None)))
      assert5()

      // Trying to put in the exact same offset, will cause us to reject it
      // requesterInfo2 with 43 stays in the requestQueue, the one trying to put this in is the one
      // that gets back that superseded message
      processor.addToQueue(DispatchCommitConsumerOffsetToKafka(consumerId, topicAndPartition2, OffsetAndMetadata(43, None), requesterInfo))
      processor.requestQueue must_== Map(consumerId -> Map(topicAndPartition -> ((OffsetAndMetadata(309, None), requesterInfo2)),
        topicAndPartition2 -> ((OffsetAndMetadata(43, None), requesterInfo2))))
      def assert6() =
        there was one(requesterInfo).!(CommitConsumerOffset.Superseded(consumerId, topicAndPartition2, OffsetAndMetadata(43, None), OffsetAndMetadata(43, None)))
      assert6()

      // A different consumer has their own set of topics and partitions, so low offsets are allowed
      val requesterInfo3 = mock[RequesterInfo]
      processor.addToQueue(DispatchCommitConsumerOffsetToKafka(consumerId2, topicAndPartition, OffsetAndMetadata(10, None), requesterInfo3))
      processor.requestQueue must_== Map(consumerId -> Map(topicAndPartition -> ((OffsetAndMetadata(309, None), requesterInfo2)),
                                                           topicAndPartition2 -> ((OffsetAndMetadata(43, None), requesterInfo2))),
        consumerId2 -> Map(topicAndPartition -> ((OffsetAndMetadata(10, None), requesterInfo3))))

      // Superseding should work properly for the other consumer id
      processor.addToQueue(DispatchCommitConsumerOffsetToKafka(consumerId2, topicAndPartition, OffsetAndMetadata(55, None), requesterInfo3))
      processor.requestQueue must_== Map(consumerId -> Map(topicAndPartition -> ((OffsetAndMetadata(309, None), requesterInfo2)),
                                                           topicAndPartition2 -> ((OffsetAndMetadata(43, None), requesterInfo2))),
        consumerId2 -> Map(topicAndPartition -> ((OffsetAndMetadata(55, None), requesterInfo3))))
      def assert7() =
        there was one(requesterInfo3).!(CommitConsumerOffset.Superseded(consumerId2, topicAndPartition, OffsetAndMetadata(10, None), OffsetAndMetadata(55, None)))
      assert7()

      // Repeat all the asserts to make sure there were no other weird side effects from calls after
      // the initial assert
      assert1()
      assert2()
      assert3()
      assert4()
      assert5()
      assert6()
      assert7()

    }

    "handle leader not available properly" in {
      val kafkaDispatcher = mock[KafkaDispatcherRef]
      val metricsLogger = new MockMetricsLogger()
      val processor = new CommitConsumerOffsetProcessor(kafkaDispatcher, mock[CastleSimpleConsumer], metricsLogger)

      val requesterInfo = mock[RequesterInfo]
      val requesterInfo2 = mock[RequesterInfo]
      processor.requestQueue = LinkedHashMap(consumerId -> Map(topicAndPartition -> ((OffsetAndMetadata(309L, None), requesterInfo)),
        topicAndPartition2 -> ((OffsetAndMetadata(43L, None), requesterInfo))),
        consumerId2 -> Map(topicAndPartition -> ((OffsetAndMetadata(10L, None), requesterInfo2))))

      // Make sure the request queue is correctly modified for leader not available
      processor.handleLeaderNotAvailable(consumerId, OffsetAndMetadata(35282, None), requesterInfo, topicAndPartition)
      processor.requestQueue must_== LinkedHashMap(consumerId -> Map(topicAndPartition2 -> ((OffsetAndMetadata(43, None), requesterInfo))))

      there was one(kafkaDispatcher).!(LeaderNotAvailable(
        Queue(
          DispatchCommitConsumerOffsetToKafka(consumerId, topicAndPartition, OffsetAndMetadata(35282, None), requesterInfo),
          DispatchCommitConsumerOffsetToKafka(consumerId, topicAndPartition, OffsetAndMetadata(309, None), requesterInfo),
          DispatchCommitConsumerOffsetToKafka(consumerId2, topicAndPartition, OffsetAndMetadata(10, None), requesterInfo2)
      )))
    }

    "send a refresh brokers request when unknown topics are encountered" in new actorSystem {
      val kafkaDispatcher = mock[KafkaDispatcherRef]
      val metricsLogger = new MockMetricsLogger()
      val processor = new CommitConsumerOffsetProcessor(kafkaDispatcher, mock[CastleSimpleConsumer], metricsLogger)

      val offsetCommitResponse = new OffsetCommitResponse(Map(topicAndPartition -> ErrorMapping.UnknownTopicOrPartitionCode))
      val mockRequesterActorRefOriginal = TestProbe()
      val requesterInfo = RequesterInfo(mockRequesterActorRefOriginal.ref)
      val response = CommitConsumerOffsetKafkaResponse(consumerId,
        offsetCommitResponse, Map(topicAndPartition -> (OffsetAndMetadata(43, None), requesterInfo)))

      processor.processResponse(response)
      there was one(kafkaDispatcher).!(RefreshBrokersAndLeaders(List.empty))
    }
  }
}
