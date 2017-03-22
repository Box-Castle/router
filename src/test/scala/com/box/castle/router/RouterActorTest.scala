package com.box.castle.router

import java.util.concurrent.{Semaphore, TimeUnit}

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestProbe}
import com.box.castle.batch.CastleMessageBatch
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.kafkadispatcher.KafkaDispatcherFactory
import com.box.castle.router.messages._
import com.box.castle.router.metrics.Metrics
import com.box.castle.router.mock._
import com.box.castle.router.proxy.KafkaDispatcherProxyPoolFactory
import com.box.kafka.Broker
import com.box.castle.consumer._
import kafka.api._
import kafka.cluster.KafkaConversions
import kafka.common.{ErrorMapping, OffsetMetadataAndError, TopicAndPartition}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.time.NoTimeConversions

import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration

/**
 * Created by dgrenader on 9/23/14.
 */
class RouterActorTest extends Specification with Mockito with MockBatchTools
    with MockActorTools with NoTimeConversions {

  val err = ErrorMapping
  
  val timeout = FiniteDuration(10, TimeUnit.SECONDS)

  val fetchSyncInbound = new Object()

  val consumerId = ConsumerId("mock_consumer")

  val defaultTopicAndPartition = TopicAndPartition("perf", 1)
  val defaultTopicAndPartition2 = TopicAndPartition("api", 17)

  val defaultBroker1 = Broker(0, "mock.dev.box.net", 8000)
  val defaultKafkaBroker1 = KafkaConversions.brokerToKafkaBroker(defaultBroker1)

  val defaultBroker2 = Broker(0, "mock-2.dev.box.net", 8000)
  val kafkaBroker2 = KafkaConversions.brokerToKafkaBroker(defaultBroker2)

  val replicaBroker = Broker(0, "REPLICA-broker.dev.box.net", 8000)
  val kafkaReplicaBroker = KafkaConversions.brokerToKafkaBroker(replicaBroker)

  val isrBroker = Broker(0, "ISR-broker.dev.box.net", 8000)
  val kafkaIsrBroker = KafkaConversions.brokerToKafkaBroker(isrBroker)

  val unknownErrorCodeForTopic = TopicMetadata(defaultTopicAndPartition.topic,
    Seq(PartitionMetadata(defaultTopicAndPartition.partition, Some(defaultKafkaBroker1),
      isr = Seq(kafkaIsrBroker),
      replicas = Seq(kafkaReplicaBroker),
      errorCode = err.UnknownCode)))

  val unknownErrorCodeForMetadata = TopicMetadata(defaultTopicAndPartition.topic,
    Seq(PartitionMetadata(defaultTopicAndPartition.partition, Some(defaultKafkaBroker1),
      isr = Seq(kafkaIsrBroker),
      replicas = Seq(kafkaReplicaBroker))), errorCode = err.UnknownCode)

  val topicAndPartitionMetadata = TopicMetadata(defaultTopicAndPartition.topic,
    Seq(PartitionMetadata(defaultTopicAndPartition.partition, Some(defaultKafkaBroker1),
      isr = Seq(kafkaIsrBroker),
      replicas = Seq(kafkaReplicaBroker))))

  val topicAndPartition2Metadata = TopicMetadata(defaultTopicAndPartition2.topic,
    Seq(PartitionMetadata(defaultTopicAndPartition2.partition, Some(defaultKafkaBroker1),
      isr = Seq(kafkaIsrBroker),
      replicas = Seq(kafkaReplicaBroker))))

  val defaultMockTopicMetadata: Map[Int, TopicMetadataResponse] =
    Map(1 -> TopicMetadataResponse(Seq(unknownErrorCodeForTopic), 1),
        2 -> TopicMetadataResponse(Seq(unknownErrorCodeForMetadata), 2),
        3 -> TopicMetadataResponse(Seq(topicAndPartitionMetadata, topicAndPartition2Metadata), 3))

  val emptyMockTopicMetadata = Map.empty[Int, TopicMetadataResponse]

  // The first time the topic partition points at broker 1
  val broker1TopicMetadataResponse1 = TopicMetadata(defaultTopicAndPartition.topic,
    Seq(PartitionMetadata(defaultTopicAndPartition.partition, Some(defaultKafkaBroker1), replicas = Seq.empty)))
  val broker1TopicMetadataResponse1forDefaultTopicAndPartition2 = TopicMetadata(defaultTopicAndPartition2.topic,
    Seq(PartitionMetadata(defaultTopicAndPartition2.partition, Some(defaultKafkaBroker1), replicas = Seq.empty)))


  // The second time around it will point to broker 2
  val broker1TopicMetadataResponse2 = TopicMetadata(defaultTopicAndPartition.topic,
    Seq(PartitionMetadata(defaultTopicAndPartition.partition, Some(kafkaBroker2), replicas = Seq.empty)))

  val broker1TopicMetadataResponse2forDefaultTopicAndPartition2 = TopicMetadata(defaultTopicAndPartition2.topic,
    Seq(PartitionMetadata(defaultTopicAndPartition2.partition, Some(kafkaBroker2), replicas = Seq.empty)))

  val leaderChangeMockTopicMetadata = Map(
    1 -> TopicMetadataResponse(Seq(broker1TopicMetadataResponse1, broker1TopicMetadataResponse1forDefaultTopicAndPartition2), 1),
    2 -> TopicMetadataResponse(Seq(broker1TopicMetadataResponse2, broker1TopicMetadataResponse2forDefaultTopicAndPartition2), 2))

  val offset: Long = 1000L
  val a = makeMockMessageAndOffset(offset, offset + 1, 20)
  val b = makeMockMessageAndOffset(offset + 1, offset + 2, 17)
  val c = makeMockMessageAndOffset(offset + 2, offset + 3, 36)

  val defaultMessageSet = makeMockMessageSet(List(a, b, c))
  val defaultPartitionData = FetchResponsePartitionData(messages=defaultMessageSet)

  def errorFetchResponse(errorCode: Short) = {
    FetchResponsePartitionData(error=errorCode, messages=makeMockMessageSet(List(a, b, c, b, c, a)))
  }

  def createRouterActor[T <: akka.actor.Actor](mockMetricsLogger: MetricsLogger,
                                               mockBoxSimpleConsumer: MockCastleSimpleConsumer)
                                              (implicit system: ActorSystem): TestActorRef[T] = {
    val broker = mockBoxSimpleConsumer.broker
    val boxSimpleConsumerFactory = new MockCastleSimpleConsumerFactory(Map(broker -> mockBoxSimpleConsumer))
    createRouterActor[T](boxSimpleConsumerFactory, mockMetricsLogger, Set(broker))
  }

  def createRouterActor[T <: akka.actor.Actor](boxSimpleConsumerFactory: CastleSimpleConsumerFactory,
                                               mockMetricsLogger: MetricsLogger,
                                               brokers: Set[Broker])(implicit system: ActorSystem): TestActorRef[T] = {
    val kafkaDispatcherFactory = new KafkaDispatcherFactory(boxSimpleConsumerFactory, mockMetricsLogger)
    val kafkaDispatcherProxyPoolFactory = new KafkaDispatcherProxyPoolFactory(kafkaDispatcherFactory, 1024 * 1024, mockMetricsLogger)
    val routerActorFactory = new RouterFactory(kafkaDispatcherProxyPoolFactory, brokers, mockMetricsLogger)

    val requester = TestProbe()
    TestActorRef[T](routerActorFactory.props(), requester.ref, "router")
  }

  "FetchData" should {
    "process FetchData requests properly" in new actorSystem {
      val mockMetricsLogger = new MockMetricsLogger()

      val zeroMessagesSet = makeMockMessageSet(List.empty)
      val zeroMessagesPartitionData = FetchResponsePartitionData(messages=zeroMessagesSet)

      val zz = makeMockMessageAndOffset(offset + 249, offset + 250, 20)
      val zz2 = makeMockMessageAndOffset(offset + 250, offset + 251, 17)
      val zz3 = makeMockMessageAndOffset(offset + 251, offset + 252, 36)

      val invalidMessageSet = makeMockMessageSet(List(zz, zz2, zz3))
      val invalidMessageSetPartitionData = FetchResponsePartitionData(messages=invalidMessageSet, error=err.InvalidMessageCode)

      val mockFetchData =
        Map(1 -> Map(defaultTopicAndPartition -> Map(offset -> errorFetchResponse(err.UnknownCode))),
            2 -> Map(defaultTopicAndPartition -> Map(offset -> defaultPartitionData)),
            3 -> Map(defaultTopicAndPartition -> Map(offset + 3 -> zeroMessagesPartitionData)),
            4 -> Map(defaultTopicAndPartition -> Map(offset + 13 -> errorFetchResponse(err.OffsetOutOfRangeCode))),
            5 -> Map(defaultTopicAndPartition -> Map(offset + 27 -> errorFetchResponse(err.UnknownTopicOrPartitionCode))),
            6 -> Map(defaultTopicAndPartition -> Map(offset + 249 -> invalidMessageSetPartitionData)))

      val mockBoxSimpleConsumer = new MockCastleSimpleConsumer(timeout, defaultBroker1, defaultMockTopicMetadata,
        mockFetchData)

      val routerActor = createRouterActor[Router](mockMetricsLogger, mockBoxSimpleConsumer)
      val requester = TestProbe()
      implicit val sender = requester.ref

      // Success
      routerActor ! FetchData(defaultTopicAndPartition, offset)
      val expectedCastleMessageBatch = CastleMessageBatch(defaultMessageSet)
      val expectedResult = FetchData.Success(defaultTopicAndPartition, offset, expectedCastleMessageBatch)
      requester.expectMsg(timeout, expectedResult)

      // No messages
      routerActor ! FetchData(defaultTopicAndPartition, offset + 3)
      requester.expectMsg(timeout, FetchData.NoMessages(defaultTopicAndPartition, offset + 3))

      // Offset out of range
      routerActor ! FetchData(defaultTopicAndPartition, offset + 13)
      requester.expectMsg(timeout, FetchData.OffsetOutOfRange(defaultTopicAndPartition, offset + 13))

      // Unknown topic or partition
      routerActor ! FetchData(defaultTopicAndPartition, offset + 27)
      requester.expectMsg(timeout, FetchData.UnknownTopicOrPartition(defaultTopicAndPartition, offset + 27))

      // This would get serviced out of cache
      mockMetricsLogger.getCountFor(Metrics.CacheHits) must_== 0
      routerActor ! FetchData(defaultTopicAndPartition, offset)
      requester.expectMsg(timeout, expectedResult)
      mockMetricsLogger.getCountFor(Metrics.CacheHits) must_== 1

      // Cache should work for misaligned requests as well
      val expectedResult2 = FetchData.Success(defaultTopicAndPartition, offset + 1,
        CastleMessageBatch(makeMockMessageSet(List(b, c))))
      routerActor ! FetchData(defaultTopicAndPartition, offset + 1)
      requester.expectMsg(timeout, expectedResult2)
      mockMetricsLogger.getCountFor(Metrics.CacheHits) must_== 2

      // Cache should work for misaligned requests as well
      val expectedResult3 = FetchData.Success(defaultTopicAndPartition, offset + 2,
        CastleMessageBatch(makeMockMessageSet(List(c))))
      routerActor ! FetchData(defaultTopicAndPartition, offset + 2)
      requester.expectMsg(timeout, expectedResult3)
      mockMetricsLogger.getCountFor(Metrics.CacheHits) must_== 3

      routerActor ! FetchData(defaultTopicAndPartition, offset + 249)
      val invalidExpectedCastleMessageBatch = CastleMessageBatch(invalidMessageSet)
      requester.expectMsg(timeout, FetchData.Success(defaultTopicAndPartition, offset + 249, invalidExpectedCastleMessageBatch))

      mockBoxSimpleConsumer.getNumErrors must_== 0
    }

    "correctly service outstanding FetchData requests if they can be serviced with the data we just got" in new actorSystem {
      val mockMetricsLogger = new MockMetricsLogger()

      val offset2: Long = 3500L
      val messageSet2 = makeMockMessageSet(
        List(makeMockMessageAndOffset(offset2, offset2 + 1, 1210),
          makeMockMessageAndOffset(offset2 + 1, offset2 + 2, 517),
          makeMockMessageAndOffset(offset2 + 2, offset2 + 3, 346)))
      val partitionData2 = FetchResponsePartitionData(messages=messageSet2)

      val offset3: Long = 888666L
      val messageSet3 = makeMockMessageSet(
        List(makeMockMessageAndOffset(offset3, offset3 + 1, 6321),
          makeMockMessageAndOffset(offset3 + 1, offset3 + 2, 99),
          makeMockMessageAndOffset(offset3 + 2, offset3 + 3, 832)))
      val partitionData3 = FetchResponsePartitionData(messages=messageSet3)

      val mockFetchData =
        Map(1 -> Map(defaultTopicAndPartition  -> Map(offset  -> defaultPartitionData)),
          2 -> Map(defaultTopicAndPartition  -> Map(offset2 -> partitionData2),
            defaultTopicAndPartition2 -> Map(offset3 -> partitionData3)))

      val requester = TestProbe()
      val syncClient = new Semaphore(0)
      val syncServer = new Semaphore(0)
      val mockBoxSimpleConsumer = new MockCastleSimpleConsumer(timeout, defaultBroker1, defaultMockTopicMetadata,
        mockFetchData, fetchSyncOption = Some((syncClient, syncServer)))
      val routerActor = createRouterActor[Router](mockMetricsLogger, mockBoxSimpleConsumer)
      implicit val sender = requester.ref

      // Success
      routerActor ! FetchData(defaultTopicAndPartition, offset)

      // Wait for the mock consumer to tell us it's ready to start processing fetch
      syncClient.tryAcquire(10, TimeUnit.SECONDS)

      // At this point the server is in the middle of processing the request

      // These requests will get put into the queue
      routerActor ! FetchData(defaultTopicAndPartition, offset)

      routerActor ! FetchData(defaultTopicAndPartition, offset + 1) // slice into the result
      routerActor ! FetchData(defaultTopicAndPartition, offset2)
      routerActor ! FetchData(defaultTopicAndPartition2, offset3)
      routerActor ! FetchData(defaultTopicAndPartition, offset + 2) // slice into the result

      // We release this semaphore so we are ready for the next iteration
      syncClient.release()

      // Tell the mock consumer it can continue processing fetch
      syncServer.release()

      val expectedCastleMessageBatch = CastleMessageBatch(defaultMessageSet)
      val expectedResult = FetchData.Success(defaultTopicAndPartition, offset, expectedCastleMessageBatch)

      // This will happen twice since we requested it twice
      requester.expectMsg(timeout, expectedResult)
      requester.expectMsg(timeout, expectedResult)

      val expectedResult2 = FetchData.Success(defaultTopicAndPartition, offset + 1,
        CastleMessageBatch(makeMockMessageSet(List(b, c))))
      requester.expectMsg(timeout, expectedResult2)

      val expectedResult3 = FetchData.Success(defaultTopicAndPartition, offset + 2,
        CastleMessageBatch(makeMockMessageSet(List(c))))
      requester.expectMsg(timeout, expectedResult3)


      mockMetricsLogger.getCountFor(Metrics.CacheHits) must_== 0
      mockMetricsLogger.getCountFor(Metrics.BatchedRequests) must_== 1

      // This is the key thing we are looking for
      mockMetricsLogger.getCountFor(Metrics.AvoidedFetches) must_== 3

      // Wait for the mock consumer to tell us it's ready to start processing fetch
      syncClient.tryAcquire(10, TimeUnit.SECONDS)
      syncClient.release()

      // Tell the server it can continue processing the request
      syncServer.release()

      requester.expectMsg(timeout, FetchData.Success(defaultTopicAndPartition, offset2,
        CastleMessageBatch(messageSet2)))

      requester.expectMsg(timeout, FetchData.Success(defaultTopicAndPartition2, offset3,
        CastleMessageBatch(messageSet3)))

      mockMetricsLogger.getCountFor(Metrics.NumFetches) must_== 2
      mockMetricsLogger.getCountFor(Metrics.CacheHits) must_== 0
      mockMetricsLogger.getCountFor(Metrics.BatchedRequests) must_== 3
      mockMetricsLogger.getCountFor(Metrics.AvoidedFetches) must_== 3

      mockBoxSimpleConsumer.getNumErrors must_== 0
    }

    "correctly handle leader changes during FetchData requests" in new actorSystem {
      val mockMetricsLogger = new MockMetricsLogger()

      val broker1MockFetchData =
        Map(1 -> Map(defaultTopicAndPartition -> Map(offset -> errorFetchResponse(err.NotLeaderForPartitionCode))))

      val broker2MessageSet = makeMockMessageSet(List(a, b, c))
      val broker2PartitionData = FetchResponsePartitionData(messages=broker2MessageSet)

      val broker2MockFetchData = Map(1 -> Map(defaultTopicAndPartition -> Map(offset -> broker2PartitionData)))

      val mockBoxSimpleConsumer1 = new MockCastleSimpleConsumer(timeout, defaultBroker1, leaderChangeMockTopicMetadata, broker1MockFetchData)

      val mockBoxSimpleConsumer2 = new MockCastleSimpleConsumer(timeout, defaultBroker2, emptyMockTopicMetadata, broker2MockFetchData)

      val boxSimpleConsumerFactory = mock[CastleSimpleConsumerFactory]
      boxSimpleConsumerFactory.create(defaultBroker1) returns mockBoxSimpleConsumer1
      boxSimpleConsumerFactory.create(defaultBroker2) returns mockBoxSimpleConsumer2

      val requester = TestProbe()
      val routerActor = createRouterActor[Router](boxSimpleConsumerFactory, mockMetricsLogger, Set(defaultBroker1))

      implicit val sender = requester.ref

      // Success
      routerActor ! FetchData(defaultTopicAndPartition, offset)

      val expectedCastleMessageBatch = CastleMessageBatch(broker2MessageSet)
      val expectedResult = FetchData.Success(defaultTopicAndPartition, offset, expectedCastleMessageBatch)

      requester.expectMsg(timeout, expectedResult)

      mockMetricsLogger.getCountFor(Metrics.NumFetches) must_== 2
      mockMetricsLogger.getCountFor(Metrics.CacheHits) must_== 0
      mockMetricsLogger.getCountFor(Metrics.BatchedRequests) must_== 1
      mockMetricsLogger.getCountFor(Metrics.AvoidedFetches) must_== 0

      mockBoxSimpleConsumer1.getNumErrors must_== 0
      mockBoxSimpleConsumer2.getNumErrors must_== 0
    }

    "correctly handle leader changes with outstanding requests for FetchData" in new actorSystem {
      val mockMetricsLogger = new MockMetricsLogger()

      val broker1MockFetchData =
        Map(1 -> Map(defaultTopicAndPartition -> Map(offset -> errorFetchResponse(err.NotLeaderForPartitionCode))))

      val broker2MessageSet = makeMockMessageSet(List(a, b, c))
      val broker2PartitionData = FetchResponsePartitionData(messages=broker2MessageSet)

      val broker2MockFetchData = Map(1 ->  Map(defaultTopicAndPartition -> Map(offset -> broker2PartitionData)))

      val syncClient = new Semaphore(0)
      val syncServer = new Semaphore(0)

      val mockBoxSimpleConsumer1 = new MockCastleSimpleConsumer(timeout,
        defaultBroker1,
        leaderChangeMockTopicMetadata,
        broker1MockFetchData,
        fetchSyncOption = Some((syncClient, syncServer)))

      val mockBoxSimpleConsumer2 = new MockCastleSimpleConsumer(timeout,
        defaultBroker2,
        emptyMockTopicMetadata,
        broker2MockFetchData)
      val boxSimpleConsumerFactory = mock[CastleSimpleConsumerFactory]
      boxSimpleConsumerFactory.create(defaultBroker1) returns mockBoxSimpleConsumer1
      boxSimpleConsumerFactory.create(defaultBroker2) returns mockBoxSimpleConsumer2

      val requester = TestProbe()
      val routerActor = createRouterActor[Router](boxSimpleConsumerFactory, mockMetricsLogger, Set(defaultBroker1))
      implicit val sender = requester.ref

      // Success
      routerActor ! FetchData(defaultTopicAndPartition, offset)
      // Wait for the mock consumer to tell us it's ready to start processing fetch
      syncClient.tryAcquire(10, TimeUnit.SECONDS)

      // These requests will get put into the queue
      routerActor ! FetchData(defaultTopicAndPartition, offset + 1) // slice into the result
      routerActor ! FetchData(defaultTopicAndPartition, offset + 2) // slice into the result

      syncClient.release()
      // Tell the mock consumer it can continue processing fetch
      syncServer.release()

      val expectedCastleMessageBatch = CastleMessageBatch(broker2MessageSet)
      val expectedResult = FetchData.Success(defaultTopicAndPartition, offset, expectedCastleMessageBatch)

      requester.expectMsg(timeout, expectedResult)

      val expectedResult2 = FetchData.Success(defaultTopicAndPartition, offset + 1,
        CastleMessageBatch(makeMockMessageSet(List(b, c))))
      requester.expectMsg(timeout, expectedResult2)

      val expectedResult3 = FetchData.Success(defaultTopicAndPartition, offset + 2,
        CastleMessageBatch(makeMockMessageSet(List(c))))
      requester.expectMsg(timeout, expectedResult3)

      mockMetricsLogger.getCountFor(Metrics.NumFetches) must_== 2
      mockMetricsLogger.getCountFor(Metrics.CacheHits) must_== 0
      mockMetricsLogger.getCountFor(Metrics.BatchedRequests) must_== 1
      mockMetricsLogger.getCountFor(Metrics.AvoidedFetches) must_== 2

      mockBoxSimpleConsumer1.getNumErrors must_== 0
      mockBoxSimpleConsumer2.getNumErrors must_== 0
    }

    "correctly service outstanding FetchData requests when when we get NoMessages back" in new actorSystem {
      val mockMetricsLogger = new MockMetricsLogger()

      val noMessagesPartitionData = FetchResponsePartitionData(messages=makeMockMessageSet(List.empty))

      val mockFetchData = Map(1 -> Map(defaultTopicAndPartition -> Map(offset -> noMessagesPartitionData)))

      val syncClient = new Semaphore(0)
      val syncServer = new Semaphore(0)
      val requester = TestProbe()
      val requester2 = TestProbe()
      val requester3 = TestProbe()
      val mockBoxSimpleConsumer = new MockCastleSimpleConsumer(timeout,
        defaultBroker1, defaultMockTopicMetadata, mockFetchData, fetchSyncOption = Some((syncClient, syncServer)))

      val routerActor = createRouterActor[Router](mockMetricsLogger, mockBoxSimpleConsumer)
      implicit val sender = requester.ref

      // Success
      routerActor ! FetchData(defaultTopicAndPartition, offset)

      // Wait for the mock consumer to tell us it's ready to start processing fetch
      syncClient.tryAcquire(10, TimeUnit.SECONDS)

      // These requests will get put into the queue
      routerActor.!(FetchData(defaultTopicAndPartition, offset))(requester.ref)
      routerActor.!(FetchData(defaultTopicAndPartition, offset))(requester2.ref)
      routerActor.!(FetchData(defaultTopicAndPartition, offset))(requester3.ref)

      syncClient.release()
      // Tell the mock consumer it can continue processing fetch
      syncServer.release()

      requester.expectMsg(timeout, FetchData.NoMessages(defaultTopicAndPartition, offset))
      requester.expectMsg(timeout, FetchData.NoMessages(defaultTopicAndPartition, offset))
      requester2.expectMsg(timeout, FetchData.NoMessages(defaultTopicAndPartition, offset))
      requester3.expectMsg(timeout, FetchData.NoMessages(defaultTopicAndPartition, offset))


      mockMetricsLogger.getCountFor(Metrics.NumFetches) must_== 1
      mockMetricsLogger.getCountFor(Metrics.CacheHits) must_== 0
      mockMetricsLogger.getCountFor(Metrics.BatchedRequests) must_== 1
      mockMetricsLogger.getCountFor(Metrics.AvoidedFetches) must_== 3

      mockBoxSimpleConsumer.getNumErrors must_== 0
    }
  }

  "FetchOffset" should {
    "process FetchOffset requests properly" in new actorSystem {
      val mockMetricsLogger = new MockMetricsLogger()

      val oldestOffset = PartitionOffsetsResponse(err.NoError, Seq(23842L))
      val latestOffset = PartitionOffsetsResponse(err.NoError, Seq(7390023L))
      val latestOffsetWithInvalidCode = PartitionOffsetsResponse(err.InvalidMessageCode, Seq(4598L))
      val unknownCode = PartitionOffsetsResponse(err.UnknownCode, Seq(5555L))
      val unknownTopicOrPartition = PartitionOffsetsResponse(err.UnknownTopicOrPartitionCode, Seq(888888L))

      val mockOffsetData: Map[Int, Map[TopicAndPartition, Map[OffsetType, PartitionOffsetsResponse]]] =
        Map(1 -> Map(defaultTopicAndPartition -> Map(EarliestOffset -> unknownCode)),
            2 -> Map(defaultTopicAndPartition -> Map(EarliestOffset -> oldestOffset)),
            3 -> Map(defaultTopicAndPartition -> Map(LatestOffset -> latestOffset)),
            4 -> Map(defaultTopicAndPartition -> Map(LatestOffset -> latestOffsetWithInvalidCode)),
            5 -> Map(defaultTopicAndPartition -> Map(LatestOffset -> unknownTopicOrPartition)))

      val mockBoxSimpleConsumer = new MockCastleSimpleConsumer(timeout, defaultBroker1, defaultMockTopicMetadata,
        fetchOffsetMockData=mockOffsetData)

      val requester = TestProbe()(system)
      val routerActor = createRouterActor[Router](mockMetricsLogger, mockBoxSimpleConsumer)
      implicit val sender = requester.ref

      // Success
      routerActor ! FetchOffset(EarliestOffset, defaultTopicAndPartition)
      requester.expectMsg(timeout, FetchOffset.Success(EarliestOffset, defaultTopicAndPartition, 23842L))

      routerActor ! FetchOffset(LatestOffset, defaultTopicAndPartition)
      requester.expectMsg(timeout, FetchOffset.Success(LatestOffset, defaultTopicAndPartition, 7390023L))

      routerActor ! FetchOffset(LatestOffset, defaultTopicAndPartition)
      requester.expectMsg(timeout, FetchOffset.Success(LatestOffset, defaultTopicAndPartition, 4598L))

      // Unknown topic or partition
      routerActor ! FetchOffset(LatestOffset, defaultTopicAndPartition)
      requester.expectMsg(timeout, FetchOffset.UnknownTopicOrPartition(LatestOffset, defaultTopicAndPartition))

      mockBoxSimpleConsumer.getNumErrors must_== 0
    }

    "process FetchOffset requests properly in light of Leader changes and Unknown error codes" in new actorSystem {
      val mockMetricsLogger = new MockMetricsLogger()

      val notLeader = PartitionOffsetsResponse(err.NotLeaderForPartitionCode, Seq(4598L))
      val mockFetchOffsetDataNotLeader: Map[Int, Map[TopicAndPartition, Map[OffsetType, PartitionOffsetsResponse]]] =
        Map(1 -> Map(defaultTopicAndPartition -> Map(EarliestOffset -> notLeader)))

      val oldestOffset = PartitionOffsetsResponse(err.NoError, Seq(23842L))
      val latestOffset = PartitionOffsetsResponse(err.NoError, Seq(7390023L))
      val latestOffsetWithInvalidCode = PartitionOffsetsResponse(err.InvalidMessageCode, Seq(4598L))
      val unknownCode = PartitionOffsetsResponse(err.UnknownCode, Seq(5555L))
      val unknownTopicOrPartition = PartitionOffsetsResponse(err.UnknownTopicOrPartitionCode, Seq(888888L))

      val mockOffsetData: Map[Int, Map[TopicAndPartition, Map[OffsetType, PartitionOffsetsResponse]]] =
        Map(1 -> Map(defaultTopicAndPartition -> Map(EarliestOffset -> unknownCode)),
          2 -> Map(defaultTopicAndPartition -> Map(EarliestOffset -> oldestOffset)),
          3 -> Map(defaultTopicAndPartition -> Map(LatestOffset -> latestOffset)),
          4 -> Map(defaultTopicAndPartition -> Map(LatestOffset -> latestOffsetWithInvalidCode)),
          5 -> Map(defaultTopicAndPartition -> Map(LatestOffset -> unknownTopicOrPartition)))

      val mockBoxSimpleConsumer1 = new MockCastleSimpleConsumer(timeout, defaultBroker1, leaderChangeMockTopicMetadata,
        fetchOffsetMockData=mockFetchOffsetDataNotLeader)

      val mockBoxSimpleConsumer2 = new MockCastleSimpleConsumer(timeout, defaultBroker2, emptyMockTopicMetadata,
        fetchOffsetMockData=mockOffsetData)

      val boxSimpleConsumerFactory = mock[CastleSimpleConsumerFactory]
      boxSimpleConsumerFactory.create(defaultBroker1) returns mockBoxSimpleConsumer1
      boxSimpleConsumerFactory.create(defaultBroker2) returns mockBoxSimpleConsumer2

      val requester = TestProbe()
      val routerActor = createRouterActor[Router](boxSimpleConsumerFactory, mockMetricsLogger, Set(defaultBroker1))
      implicit val sender = requester.ref

      // Success
      routerActor ! FetchOffset(EarliestOffset, defaultTopicAndPartition)
      requester.expectMsg(timeout, FetchOffset.Success(EarliestOffset, defaultTopicAndPartition, 23842L))

      routerActor ! FetchOffset(LatestOffset, defaultTopicAndPartition)
      requester.expectMsg(timeout, FetchOffset.Success(LatestOffset, defaultTopicAndPartition, 7390023L))

      routerActor ! FetchOffset(LatestOffset, defaultTopicAndPartition)
      requester.expectMsg(timeout, FetchOffset.Success(LatestOffset, defaultTopicAndPartition, 4598L))

      // Unknown topic or partition
      routerActor ! FetchOffset(LatestOffset, defaultTopicAndPartition)
      requester.expectMsg(timeout, FetchOffset.UnknownTopicOrPartition(LatestOffset, defaultTopicAndPartition))

      mockBoxSimpleConsumer1.getNumErrors must_== 0
      mockBoxSimpleConsumer2.getNumErrors must_== 0
    }
  }

  "CommitConsumerOffset" should {
    "process CommitConsumerOffset requests properly in light of Leader changes and Unknown error codes" in new actorSystem {
      val mockMetricsLogger = new MockMetricsLogger()

      val mockCommitConsumerOffsetDataNotLeader =
        Map(1 -> Map(consumerId -> Map(defaultTopicAndPartition -> err.NotLeaderForPartitionCode)))

      val mockCommitConsumerOffsetData =
        Map(1 -> Map(consumerId -> Map(defaultTopicAndPartition -> err.UnknownCode)),
          2 -> Map(consumerId -> Map(defaultTopicAndPartition -> err.NoError)),
          3 -> Map(consumerId -> Map(defaultTopicAndPartition -> err.InvalidMessageCode)),
          4 -> Map(consumerId -> Map(defaultTopicAndPartition -> err.UnknownTopicOrPartitionCode)))

      val mockBoxSimpleConsumer1 = new MockCastleSimpleConsumer(timeout, defaultBroker1, leaderChangeMockTopicMetadata,
        commitConsumerOffsetMockData=mockCommitConsumerOffsetDataNotLeader)

      val mockBoxSimpleConsumer2 = new MockCastleSimpleConsumer(timeout, defaultBroker2, emptyMockTopicMetadata,
        commitConsumerOffsetMockData=mockCommitConsumerOffsetData)

      val boxSimpleConsumerFactory = mock[CastleSimpleConsumerFactory]
      boxSimpleConsumerFactory.create(defaultBroker1) returns mockBoxSimpleConsumer1
      boxSimpleConsumerFactory.create(defaultBroker2) returns mockBoxSimpleConsumer2

      val requester = TestProbe()
      val routerActor = createRouterActor[Router](boxSimpleConsumerFactory, mockMetricsLogger, Set(defaultBroker1))
      implicit val sender = requester.ref

      // Success
      routerActor ! CommitConsumerOffset(consumerId, defaultTopicAndPartition, OffsetAndMetadata(90000L, None))
      requester.expectMsg(timeout, CommitConsumerOffset.Success(consumerId, defaultTopicAndPartition, OffsetAndMetadata(90000L, None)))

      routerActor ! CommitConsumerOffset(consumerId, defaultTopicAndPartition, OffsetAndMetadata(4984L, None))
      requester.expectMsg(timeout, CommitConsumerOffset.Success(consumerId, defaultTopicAndPartition, OffsetAndMetadata(4984L, None)))

      // Unknown topic or partition
      routerActor ! CommitConsumerOffset(consumerId, defaultTopicAndPartition, OffsetAndMetadata(34293L, None))
      requester.expectMsg(timeout, CommitConsumerOffset.UnknownTopicOrPartition(consumerId, defaultTopicAndPartition, OffsetAndMetadata(34293L, None)))

      mockBoxSimpleConsumer1.getNumErrors must_== 0
      mockBoxSimpleConsumer2.getNumErrors must_== 0
    }

    "process CommitConsumerOffset during leader changes with outstanding requests" in new actorSystem {
      val mockMetricsLogger = new MockMetricsLogger()

      // consumerId will have TWO topic and partitions associated with it
      val mockCommitConsumerOffsetDataNotLeader =
        Map(1 -> Map(consumerId -> Map(defaultTopicAndPartition -> err.NotLeaderForPartitionCode)),
            2 -> Map(consumerId -> Map(defaultTopicAndPartition2 -> err.NotLeaderForPartitionCode)))

      val consumerId2 = ConsumerId("another_consumer_two")
      val consumerId3 = ConsumerId("another_consumer_three")
      val consumerId4 = ConsumerId("another_consumer_four")

      val mockCommitConsumerOffsetData =
        Map(1 -> Map(consumerId -> Map(defaultTopicAndPartition -> err.NoError,
                                       defaultTopicAndPartition2 -> err.NoError)),
          2 -> Map(consumerId -> Map(defaultTopicAndPartition -> err.NoError,
                                     defaultTopicAndPartition2 -> err.NoError)),
          3 -> Map(consumerId2 -> Map(defaultTopicAndPartition -> err.NoError)),
          4 -> Map(consumerId3 -> Map(defaultTopicAndPartition -> err.NoError)),
          5 -> Map(consumerId4 -> Map(defaultTopicAndPartition -> err.NoError)))

      val syncClient = new Semaphore(0)
      val syncServer = new Semaphore(0)

      val mockBoxSimpleConsumer1 = new MockCastleSimpleConsumer(timeout, defaultBroker1, leaderChangeMockTopicMetadata,
        commitConsumerOffsetMockData=mockCommitConsumerOffsetDataNotLeader,
        commitConsumerOffsetSyncOption = Some((syncClient, syncServer)))

      val mockBoxSimpleConsumer2 = new MockCastleSimpleConsumer(timeout, defaultBroker2, emptyMockTopicMetadata,
        commitConsumerOffsetMockData=mockCommitConsumerOffsetData)

      val boxSimpleConsumerFactory = mock[CastleSimpleConsumerFactory]
      boxSimpleConsumerFactory.create(defaultBroker1) returns mockBoxSimpleConsumer1
      boxSimpleConsumerFactory.create(defaultBroker2) returns mockBoxSimpleConsumer2

      val requester = TestProbe()
      val routerActor = createRouterActor[Router](boxSimpleConsumerFactory, mockMetricsLogger, Set(defaultBroker1))
      implicit val sender = requester.ref

      // Success
      routerActor ! CommitConsumerOffset(consumerId, defaultTopicAndPartition, OffsetAndMetadata(100, None))

      // Wait for the mock consumer to tell us it's ready to start processing fetch
      syncClient.tryAcquire(10, TimeUnit.SECONDS)

      // At this point the mock consumer is blocked and so our request is "outstanding" on the wire
      // We can simulate adding things into the request queue while this request for 100 is outstanding
      routerActor ! CommitConsumerOffset(consumerId, defaultTopicAndPartition, OffsetAndMetadata(200, None))
      // 100 will not get superseded because it is already outstaning on the wire

      routerActor ! CommitConsumerOffset(consumerId, defaultTopicAndPartition, OffsetAndMetadata(400, None))
      requester.expectMsg(timeout, CommitConsumerOffset.Superseded(consumerId, defaultTopicAndPartition, OffsetAndMetadata(200, None), OffsetAndMetadata(400, None)))
      routerActor ! CommitConsumerOffset(consumerId, defaultTopicAndPartition, OffsetAndMetadata(300, None))
      requester.expectMsg(timeout, CommitConsumerOffset.Superseded(consumerId, defaultTopicAndPartition, OffsetAndMetadata(300, None), OffsetAndMetadata(400, None)))
      routerActor ! CommitConsumerOffset(consumerId, defaultTopicAndPartition, OffsetAndMetadata(91, None))
      requester.expectMsg(timeout, CommitConsumerOffset.Superseded(consumerId, defaultTopicAndPartition, OffsetAndMetadata(91, None), OffsetAndMetadata(400, None)))

      routerActor ! CommitConsumerOffset(consumerId, defaultTopicAndPartition2, OffsetAndMetadata(56, None))
      routerActor ! CommitConsumerOffset(consumerId, defaultTopicAndPartition2, OffsetAndMetadata(66, None))
      requester.expectMsg(timeout, CommitConsumerOffset.Superseded(consumerId, defaultTopicAndPartition2, OffsetAndMetadata(56, None), OffsetAndMetadata(66, None)))

      routerActor ! CommitConsumerOffset(consumerId2, defaultTopicAndPartition, OffsetAndMetadata(1000, None))
      routerActor ! CommitConsumerOffset(consumerId2, defaultTopicAndPartition, OffsetAndMetadata(1100, None))
      requester.expectMsg(timeout, CommitConsumerOffset.Superseded(consumerId2, defaultTopicAndPartition, OffsetAndMetadata(1000, None), OffsetAndMetadata(1100, None)))
      routerActor ! CommitConsumerOffset(consumerId2, defaultTopicAndPartition, OffsetAndMetadata(1200, None))
      requester.expectMsg(timeout, CommitConsumerOffset.Superseded(consumerId2, defaultTopicAndPartition, OffsetAndMetadata(1100, None), OffsetAndMetadata(1200, None)))
      routerActor ! CommitConsumerOffset(consumerId2, defaultTopicAndPartition, OffsetAndMetadata(71, None))
      requester.expectMsg(timeout, CommitConsumerOffset.Superseded(consumerId2, defaultTopicAndPartition, OffsetAndMetadata(71, None), OffsetAndMetadata(1200, None)))

      routerActor ! CommitConsumerOffset(consumerId3, defaultTopicAndPartition, OffsetAndMetadata(2100, None))
      routerActor ! CommitConsumerOffset(consumerId3, defaultTopicAndPartition, OffsetAndMetadata(2200, None))
      requester.expectMsg(timeout, CommitConsumerOffset.Superseded(consumerId3, defaultTopicAndPartition, OffsetAndMetadata(2100, None), OffsetAndMetadata(2200, None)))
      routerActor ! CommitConsumerOffset(consumerId3, defaultTopicAndPartition, OffsetAndMetadata(2300, None))
      requester.expectMsg(timeout, CommitConsumerOffset.Superseded(consumerId3, defaultTopicAndPartition, OffsetAndMetadata(2200, None), OffsetAndMetadata(2300, None)))
      routerActor ! CommitConsumerOffset(consumerId3, defaultTopicAndPartition, OffsetAndMetadata(61, None))
      requester.expectMsg(timeout, CommitConsumerOffset.Superseded(consumerId3, defaultTopicAndPartition, OffsetAndMetadata(61, None), OffsetAndMetadata(2300, None)))

      routerActor ! CommitConsumerOffset(consumerId4, defaultTopicAndPartition, OffsetAndMetadata(12, None))
      routerActor ! CommitConsumerOffset(consumerId4, defaultTopicAndPartition, OffsetAndMetadata(13, None))
      requester.expectMsg(timeout, CommitConsumerOffset.Superseded(consumerId4, defaultTopicAndPartition, OffsetAndMetadata(12, None), OffsetAndMetadata(13, None)))
      routerActor ! CommitConsumerOffset(consumerId4, defaultTopicAndPartition, OffsetAndMetadata(14, None))
      requester.expectMsg(timeout, CommitConsumerOffset.Superseded(consumerId4, defaultTopicAndPartition, OffsetAndMetadata(13, None), OffsetAndMetadata(14, None)))
      routerActor ! CommitConsumerOffset(consumerId4, defaultTopicAndPartition, OffsetAndMetadata(7, None))
      requester.expectMsg(timeout, CommitConsumerOffset.Superseded(consumerId4, defaultTopicAndPartition, OffsetAndMetadata(7, None), OffsetAndMetadata(14, None)))

      syncClient.release()
      // Tell the mock consumer it can continue processing fetch
      syncServer.release()
      // At this point the mock committer is unblocked by us, and will return a result to us

      // This is for defaultTopicAndPartition2 request going to broker 1
      syncServer.release()

      requester.expectMsg(timeout, CommitConsumerOffset.Success(consumerId, defaultTopicAndPartition, OffsetAndMetadata(100, None)))

      requester.expectMsg(timeout, CommitConsumerOffset.Success(consumerId, defaultTopicAndPartition, OffsetAndMetadata(400, None)))
      requester.expectMsg(timeout, CommitConsumerOffset.Success(consumerId, defaultTopicAndPartition2, OffsetAndMetadata(66, None)))

      requester.expectMsg(timeout, CommitConsumerOffset.Success(consumerId2, defaultTopicAndPartition, OffsetAndMetadata(1200, None)))
      requester.expectMsg(timeout, CommitConsumerOffset.Success(consumerId3, defaultTopicAndPartition, OffsetAndMetadata(2300, None)))
      requester.expectMsg(timeout, CommitConsumerOffset.Success(consumerId4, defaultTopicAndPartition, OffsetAndMetadata(14, None)))

      mockBoxSimpleConsumer1.getNumErrors must_== 0
      mockBoxSimpleConsumer2.getNumErrors must_== 0
    }
  }

  "FetchConsumerOffset" should {
    "process FetchConsumerOffset requests properly" in new actorSystem {
      val mockMetricsLogger = new MockMetricsLogger()

      val mockFetchConsumerOffsetData =
        Map(1 -> Map(consumerId ->
          Map(defaultTopicAndPartition -> OffsetMetadataAndError(2222L, error = err.UnknownCode))),
          2 -> Map(consumerId ->
            Map(defaultTopicAndPartition -> OffsetMetadataAndError(77744L))),
          3 -> Map(consumerId ->
            Map(defaultTopicAndPartition -> OffsetMetadataAndError(19524L, error = err.InvalidMessageCode))),
          4 -> Map(consumerId ->
            Map(defaultTopicAndPartition -> OffsetMetadataAndError(11111L, error = err.UnknownTopicOrPartitionCode))))

      val mockBoxSimpleConsumer = new MockCastleSimpleConsumer(timeout, defaultBroker1, defaultMockTopicMetadata,
        fetchConsumerOffsetMockData = mockFetchConsumerOffsetData)

      val requester = TestProbe()
      val routerActor = createRouterActor[Router](mockMetricsLogger, mockBoxSimpleConsumer)
      implicit val sender = requester.ref

      // Success
      routerActor ! FetchConsumerOffset(consumerId, defaultTopicAndPartition)
      requester.expectMsg(timeout, FetchConsumerOffset.Success(consumerId, defaultTopicAndPartition, OffsetAndMetadata(77744L, None)))

      // Success
      routerActor ! FetchConsumerOffset(consumerId, defaultTopicAndPartition)
      requester.expectMsg(timeout, FetchConsumerOffset.Success(consumerId, defaultTopicAndPartition, OffsetAndMetadata(19524L, None)))

      // Not found
      routerActor ! FetchConsumerOffset(consumerId, defaultTopicAndPartition)
      requester.expectMsg(timeout, FetchConsumerOffset.NotFound(consumerId, defaultTopicAndPartition))

      mockBoxSimpleConsumer.getNumErrors must_== 0
    }

    "process FetchConsumerOffset requests properly in light of Leader changes and Unknown error codes" in new actorSystem {
      val mockMetricsLogger = new MockMetricsLogger()

      val mockFetchConsumerOffsetDataNotLeader =
        Map(1 -> Map(consumerId -> Map(defaultTopicAndPartition -> OffsetMetadataAndError(2222L, error = err.NotLeaderForPartitionCode))))

      val mockFetchConsumerOffsetData =
        Map(1 -> Map(consumerId ->
          Map(defaultTopicAndPartition -> OffsetMetadataAndError(2222L, error = err.UnknownCode))),
          2 -> Map(consumerId ->
            Map(defaultTopicAndPartition -> OffsetMetadataAndError(77744L))),
          3 -> Map(consumerId ->
            Map(defaultTopicAndPartition -> OffsetMetadataAndError(11111L, error = err.UnknownTopicOrPartitionCode))))

      val mockBoxSimpleConsumer1 = new MockCastleSimpleConsumer(timeout, defaultBroker1, leaderChangeMockTopicMetadata,
        fetchConsumerOffsetMockData=mockFetchConsumerOffsetDataNotLeader)

      val mockBoxSimpleConsumer2 = new MockCastleSimpleConsumer(timeout, defaultBroker2, emptyMockTopicMetadata,
        fetchConsumerOffsetMockData=mockFetchConsumerOffsetData)

      val boxSimpleConsumerFactory = mock[CastleSimpleConsumerFactory]
      boxSimpleConsumerFactory.create(defaultBroker1) returns mockBoxSimpleConsumer1
      boxSimpleConsumerFactory.create(defaultBroker2) returns mockBoxSimpleConsumer2

      val requester = TestProbe()
      val routerActor = createRouterActor[Router](boxSimpleConsumerFactory, mockMetricsLogger, Set(defaultBroker1))
      implicit val sender = requester.ref

      // Success
      routerActor ! FetchConsumerOffset(consumerId, defaultTopicAndPartition)
      requester.expectMsg(timeout, FetchConsumerOffset.Success(consumerId, defaultTopicAndPartition, OffsetAndMetadata(77744L, None)))

      // Not found
      routerActor ! FetchConsumerOffset(consumerId, defaultTopicAndPartition)
      requester.expectMsg(timeout, FetchConsumerOffset.NotFound(consumerId, defaultTopicAndPartition))

      mockBoxSimpleConsumer1.getNumErrors must_== 0
      mockBoxSimpleConsumer2.getNumErrors must_== 0
    }

    "process FetchConsumerOffset requests properly during leader changes with outstanding requests" in new actorSystem {
      val mockMetricsLogger = new MockMetricsLogger()

      val mockFetchConsumerOffsetDataNotLeader =
        Map(1 -> Map(consumerId -> Map(defaultTopicAndPartition -> OffsetMetadataAndError(2222L, error = err.NotLeaderForPartitionCode))))

      val consumerId2 = ConsumerId("another_consumer_two")
      val consumerId3 = ConsumerId("another_consumer_three")
      val consumerId4 = ConsumerId("another_consumer_four")

      val mockFetchConsumerOffsetData =
        Map(1 -> Map(consumerId -> Map(defaultTopicAndPartition -> OffsetMetadataAndError(2222))),
            2 -> Map(consumerId -> Map(defaultTopicAndPartition -> OffsetMetadataAndError(87326))),
            3 -> Map(consumerId2 -> Map(defaultTopicAndPartition -> OffsetMetadataAndError(11))),
            4 -> Map(consumerId3 -> Map(defaultTopicAndPartition -> OffsetMetadataAndError(33))),
            5 -> Map(consumerId4 -> Map(defaultTopicAndPartition -> OffsetMetadataAndError(44))))

      val syncClient = new Semaphore(0)
      val syncServer = new Semaphore(0)

      val mockBoxSimpleConsumer1 = new MockCastleSimpleConsumer(timeout, defaultBroker1, leaderChangeMockTopicMetadata,
        fetchConsumerOffsetMockData=mockFetchConsumerOffsetDataNotLeader,
        fetchConsumerOffsetSyncOption = Some((syncClient, syncServer)))

      val mockBoxSimpleConsumer2 = new MockCastleSimpleConsumer(timeout, defaultBroker2, emptyMockTopicMetadata,
        fetchConsumerOffsetMockData=mockFetchConsumerOffsetData)

      val boxSimpleConsumerFactory = mock[CastleSimpleConsumerFactory]
      boxSimpleConsumerFactory.create(defaultBroker1) returns mockBoxSimpleConsumer1
      boxSimpleConsumerFactory.create(defaultBroker2) returns mockBoxSimpleConsumer2


      val requester = TestProbe()
      val routerActor = createRouterActor[Router](boxSimpleConsumerFactory, mockMetricsLogger, Set(defaultBroker1))
      implicit val sender = requester.ref

      // Success
      routerActor ! FetchConsumerOffset(consumerId, defaultTopicAndPartition)

      // Wait for the mock consumer to tell us it's ready to start processing fetch
      syncClient.tryAcquire(10, TimeUnit.SECONDS)

      routerActor ! FetchConsumerOffset(consumerId, defaultTopicAndPartition)

      routerActor ! FetchConsumerOffset(consumerId2, defaultTopicAndPartition)
      routerActor ! FetchConsumerOffset(consumerId3, defaultTopicAndPartition)
      routerActor ! FetchConsumerOffset(consumerId4, defaultTopicAndPartition)

      syncClient.release()
      // Tell the mock consumer it can continue processing fetch
      syncServer.release()

      syncServer.release()

      requester.expectMsg(timeout, FetchConsumerOffset.Success(consumerId, defaultTopicAndPartition, OffsetAndMetadata(2222, None)))
      requester.expectMsg(timeout, FetchConsumerOffset.Success(consumerId, defaultTopicAndPartition, OffsetAndMetadata(87326, None)))
      requester.expectMsg(timeout, FetchConsumerOffset.Success(consumerId2, defaultTopicAndPartition, OffsetAndMetadata(11, None)))
      requester.expectMsg(timeout, FetchConsumerOffset.Success(consumerId3, defaultTopicAndPartition, OffsetAndMetadata(33, None)))
      requester.expectMsg(timeout, FetchConsumerOffset.Success(consumerId4, defaultTopicAndPartition, OffsetAndMetadata(44, None)))

      mockBoxSimpleConsumer1.getNumErrors must_== 0
      mockBoxSimpleConsumer2.getNumErrors must_== 0
    }
  }
}
