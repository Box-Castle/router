package com.box.castle.consumer

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit._
import java.nio.file.{Path, Paths}

import com.box.castle.consumer.offsetmetadatamanager.{ZookeeperOffsetMetadataManager, ZookeeperOffsetMetadataManagerFactory}
import com.box.kafka.Broker
import kafka.api._
import kafka.cluster.KafkaConversions
import kafka.common.{OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.{Message, MessageAndOffset, MessageSet}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.concurrent.duration.{Duration, FiniteDuration}

class BoxSimpleConsumerTest extends Specification with Mockito {

  def makeMockZookeeperOffsetMetadataFactory = mock[ZookeeperOffsetMetadataManagerFactory]


  def makeMockMessageSet(
                          mockMessageAndOffsets: Iterable[MessageAndOffset]): MessageSet = {
    val m = mock[MessageSet]
    m.size returns mockMessageAndOffsets.size
    m.nonEmpty returns mockMessageAndOffsets.nonEmpty
    m.isEmpty returns mockMessageAndOffsets.isEmpty
    m.iterator returns mockMessageAndOffsets.iterator
    m.toIndexedSeq returns mockMessageAndOffsets.toIndexedSeq
    m.sizeInBytes returns MessageSet.messageSetSize(
      mockMessageAndOffsets.map(_.message))
    m.toString returns s"MockMessageSet({${mockMessageAndOffsets.mkString(";")}})"
  }

  def makeMockMessageAndOffset(offset: Long,
                               nextOffset: Long,
                               size: Int): MessageAndOffset = {
    assert(size >= 0)
    val mockMessageAndOffset = mock[MessageAndOffset]
    mockMessageAndOffset.offset returns offset
    mockMessageAndOffset.nextOffset returns nextOffset
    val message = mock[Message]
    message.size returns size
    mockMessageAndOffset.message returns message
    mockMessageAndOffset.toString returns s"MockMessageAndOffset(offset=$offset,nextOffset=$nextOffset,size=$size)"
  }

  def createConsumer(simpleConsumer: SimpleConsumer,
                     zkOffsetMetaFactory: ZookeeperOffsetMetadataManagerFactory = makeMockZookeeperOffsetMetadataFactory,
                     useKafkaOffsetMetadataManager: Map[ConsumerId, Boolean] = Map.empty): CastleSimpleConsumer = {
    val simpleConsumerFactoryMock = mock[SimpleConsumerFactory]
    simpleConsumerFactoryMock.create(broker,
      timeout,
      defaultBufferSize,
      clientId) returns simpleConsumer

    val factory = new CastleSimpleConsumerFactory(clientId,
      Some(zkOffsetMetaFactory),
      timeout,
      defaultBufferSize,
      simpleConsumerFactoryMock,
      useKafkaOffsetMetadataManager)
    factory.create(broker)
  }


  def createSimpleConsumer: SimpleConsumer = {
    val simpleConsumerMock = mock[SimpleConsumer]
    simpleConsumerMock.bufferSize returns defaultBufferSize
    simpleConsumerMock.clientId returns clientId.value
    simpleConsumerMock
  }

  val defaultBufferSize = 28539
  val broker = Broker(0, "mock.dev.box.net", 8000)
  val timeout = FiniteDuration(10, "seconds")
  val clientId = ClientId("test")
  val consumerId = ConsumerId("mock consumer id")
  val correlationId = 99
  val topicAndPartition = TopicAndPartition("perf", 14)
  val offset: Long = 17993L
  val topicMetadata: String = "metadata given by consumer"

  "BoxSimpleConsumer" should {
    "have a reasonable toString" in {
      val simpleConsumerMock = createSimpleConsumer
      simpleConsumerMock.host returns "broker-1.dev.box.net"
      simpleConsumerMock.port returns 5150

      val consumer = createConsumer(simpleConsumerMock)
      consumer.toString must_== s"${simpleConsumerMock.host}:${simpleConsumerMock.port}"
    }

    "close underlying SimpleConsumer" in {
      val simpleConsumerMock = createSimpleConsumer

      val consumer = createConsumer(simpleConsumerMock)
      consumer.close()
      there was atLeastOne(simpleConsumerMock).close
    }
    "not allow negative broker timeouts" in {

      val factory =
        new CastleSimpleConsumerFactory(clientId,
          Some(makeMockZookeeperOffsetMetadataFactory),
          FiniteDuration(-100, TimeUnit.SECONDS),
          defaultBufferSize)

      factory.create(broker) must throwA(new IllegalArgumentException(
        "requirement failed: broker timeout must be positive"))
    }

    "handle fetch data" in {
      val a = makeMockMessageAndOffset(offset, offset + 1, 20)
      val b = makeMockMessageAndOffset(offset + 1, offset + 2, 17)
      val c = makeMockMessageAndOffset(offset + 2, offset + 3, 36)

      val defaultMessageSet = makeMockMessageSet(List(a, b, c))
      val defaultPartitionData =
        FetchResponsePartitionData(messages = defaultMessageSet)

      val topicAndPartition = TopicAndPartition("perf", 14)
      val requests = Map(topicAndPartition -> offset)
      val partitionsToFetch = requests.mapValues(offset =>
        PartitionFetchInfo(offset, defaultBufferSize))
      val requestToBeFetched =
        new FetchRequest(correlationId,
          clientId.value,
          CastleSimpleConsumer.DefaultMaxWait.toMillis.toInt,
          CastleSimpleConsumer.DefaultMinBytes,
          partitionsToFetch)

      val simpleConsumerMock = createSimpleConsumer

      simpleConsumerMock.fetch(requestToBeFetched) returns FetchResponse(
        correlationId,
        Map(topicAndPartition -> defaultPartitionData))

      val simpleConsumerFactoryMock = mock[SimpleConsumerFactory]
      simpleConsumerFactoryMock.create(broker,
        timeout,
        defaultBufferSize,
        clientId) returns simpleConsumerMock

      val factory = new CastleSimpleConsumerFactory(clientId,
        Some(makeMockZookeeperOffsetMetadataFactory),
        timeout,
        defaultBufferSize,
        simpleConsumerFactoryMock)
      val consumer = factory.create(broker)

      val result =
        consumer.fetchData(Map(topicAndPartition -> offset), correlationId)
      result.correlationId must_== 99
      val messages = result.data(topicAndPartition).messages.toIndexedSeq
      messages.size must_== 3
      messages.head.message.size must_== 20
      messages.head.offset must_== offset

      messages(1).message.size must_== 17
      messages(1).offset must_== offset + 1

      messages(2).message.size must_== 36
      messages(2).offset must_== offset + 2

      there was one(simpleConsumerMock).fetch(requestToBeFetched)
    }

    "handle fetch topic metadata" in {
      val simpleConsumerMock = createSimpleConsumer

      val mockResponse = mock[TopicMetadataResponse]
      mockResponse.toString returns "unique mock response"

      val requestToBeFetched =
        new TopicMetadataRequest(TopicMetadataRequest.CurrentVersion,
          correlationId,
          clientId.value,
          Nil)
      simpleConsumerMock.send(requestToBeFetched) returns mockResponse
      val consumer = createConsumer(simpleConsumerMock)

      val result = consumer.fetchTopicMetadata(correlationId)
      result.toString must_== "unique mock response"

      there was one(simpleConsumerMock).send(requestToBeFetched)
    }

    "handle fetch offsets" in {
      val simpleConsumerMock = createSimpleConsumer

      val mockResponse = mock[OffsetResponse]
      mockResponse.toString returns "unique mock response"

      val requestInfo = Map(
        topicAndPartition -> PartitionOffsetRequestInfo(
          EarliestOffset.toLong,
          CastleSimpleConsumer.DefaultMaxNumOffsets))

      val requestToBeFetched =
        new OffsetRequest(requestInfo,
          OffsetFetchRequest.CurrentVersion,
          correlationId,
          clientId.value)
      simpleConsumerMock
        .getOffsetsBefore(requestToBeFetched) returns mockResponse
      val consumer = createConsumer(simpleConsumerMock)

      val result =
        consumer.fetchOffsets(Map(topicAndPartition -> EarliestOffset),
          correlationId)
      result.toString must_== "unique mock response"

      there was one(simpleConsumerMock).getOffsetsBefore(requestToBeFetched)
    }

    "handle fetch consumer offsets using standard kafka consumer as offsetmetadata manager" in {
      val simpleConsumerMock = createSimpleConsumer

      val mockResponse = mock[OffsetFetchResponse]
      mockResponse.toString returns "unique mock response"

      val requestToBeFetched =
        new OffsetFetchRequest(consumerId.value,
          Seq(topicAndPartition),
          OffsetFetchRequest.CurrentVersion,
          correlationId,
          clientId.value)
      simpleConsumerMock.fetchOffsets(requestToBeFetched) returns mockResponse
      val consumer = createConsumer(simpleConsumerMock)

      val result = consumer.fetchConsumerOffsets(consumerId,
        Seq(topicAndPartition),
        correlationId)
      result.toString must_== "unique mock response"

      there was one(simpleConsumerMock).fetchOffsets(requestToBeFetched)
    }

    "handle fetch consumer offsets using zookeeper offsetmetadata manager" in {
      val simpleConsumerMock = createSimpleConsumer

      val mockResponse = mock[OffsetFetchResponse]
      mockResponse.toString returns "unique mock response"

      val requestToBeFetched =
        new OffsetFetchRequest(consumerId.value,
          Seq(topicAndPartition),
          OffsetFetchRequest.CurrentVersion,
          correlationId,
          clientId.value)
      simpleConsumerMock.fetchOffsets(requestToBeFetched) returns mockResponse

      val mockZkOffsetMetadataManagerFactory = makeMockZookeeperOffsetMetadataFactory
      val mockZookeeperOffsetMetadataManager = mock[ZookeeperOffsetMetadataManager]

      mockZookeeperOffsetMetadataManager.fetchOffsets(requestToBeFetched) returns mockResponse

      mockZkOffsetMetadataManagerFactory.create(simpleConsumerMock) returns mockZookeeperOffsetMetadataManager

      val consumer = createConsumer(simpleConsumerMock,
        mockZkOffsetMetadataManagerFactory, Map(consumerId -> false))

      val result = consumer.fetchConsumerOffsets(consumerId,
        Seq(topicAndPartition),
        correlationId)
      result.toString must_== "unique mock response"

      there was no(simpleConsumerMock).fetchOffsets(requestToBeFetched)
      there was one(mockZookeeperOffsetMetadataManager).fetchOffsets(requestToBeFetched)
    }

    "handle commit consumer offsets" in {
      val simpleConsumerMock = createSimpleConsumer

      val mockResponse = mock[OffsetCommitResponse]
      mockResponse.toString returns "unique mock response"

      val requestInfo =
        Map(topicAndPartition -> OffsetMetadataAndError(offset))
      val requestToBeFetched =
        new OffsetCommitRequest(consumerId.value,
          requestInfo,
          OffsetFetchRequest.CurrentVersion,
          correlationId,
          clientId.value)

      simpleConsumerMock.commitOffsets(requestToBeFetched) returns mockResponse
      val consumer = createConsumer(simpleConsumerMock)

      val result =
        consumer.commitConsumerOffset(consumerId,
          Map(topicAndPartition -> offset),
          correlationId)
      result.toString must_== "unique mock response"

      there was one(simpleConsumerMock).commitOffsets(requestToBeFetched)
    }

    "handle commit consumer offsets with zookeeper offsetmetadata manager" in {
      val simpleConsumerMock = createSimpleConsumer

      val mockResponse = mock[OffsetCommitResponse]
      mockResponse.toString returns "unique mock response"

      val requestInfo =
        Map(topicAndPartition -> OffsetMetadataAndError(offset))
      val requestToBeFetched =
        new OffsetCommitRequest(consumerId.value,
          requestInfo,
          OffsetFetchRequest.CurrentVersion,
          correlationId,
          clientId.value)

      simpleConsumerMock.commitOffsets(requestToBeFetched) returns mockResponse

      val mockZkOffsetMetadataManagerFactory = makeMockZookeeperOffsetMetadataFactory
      val mockZookeeperOffsetMetadataManager = mock[ZookeeperOffsetMetadataManager]

      mockZookeeperOffsetMetadataManager.commitOffsets(requestToBeFetched) returns mockResponse

      mockZkOffsetMetadataManagerFactory.create(simpleConsumerMock) returns mockZookeeperOffsetMetadataManager

      val consumer = createConsumer(simpleConsumerMock,
        mockZkOffsetMetadataManagerFactory, Map(consumerId -> false))

      val result =
        consumer.commitConsumerOffset(consumerId,
          Map(topicAndPartition -> offset),
          correlationId)
      result.toString must_== "unique mock response"

      there was no(simpleConsumerMock).commitOffsets(requestToBeFetched)
      there was one(mockZookeeperOffsetMetadataManager).commitOffsets(requestToBeFetched)
    }

    "handle commit consumer offsets with metadata" in {
      val simpleConsumerMock = createSimpleConsumer

      val mockResponse = mock[OffsetCommitResponse]
      mockResponse.toString returns "unique mock response"

      val requestInfo =
        Map(topicAndPartition -> OffsetMetadataAndError(offset, topicMetadata))
      val requestToBeFetched =
        new OffsetCommitRequest(consumerId.value,
          requestInfo,
          OffsetFetchRequest.CurrentVersion,
          correlationId,
          clientId.value)

      simpleConsumerMock.commitOffsets(requestToBeFetched) returns mockResponse
      val consumer = createConsumer(simpleConsumerMock)

      val result =
        consumer.commitConsumerOffsetAndMetadata(consumerId,
          Map(topicAndPartition -> OffsetMetadataAndError(offset, topicMetadata)),
          correlationId)
      result.toString must_== "unique mock response"

      there was one(simpleConsumerMock).commitOffsets(requestToBeFetched)
    }
  }

  "ClientID" should {
    "have a reasonable toString" in {
      clientId.value must_== clientId.toString
    }
  }

  "ConsumerId" should {
    "have a reasonable toString" in {
      consumerId.value must_== consumerId.toString
    }
  }

  "KafkaConversions" should {
    "work properly" in {
      val kafkaBroker = KafkaConversions.brokerToKafkaBroker(broker)
      val boxBroker = KafkaConversions.kafkaBrokerToBroker(kafkaBroker)

      boxBroker.id must_== broker.id
      boxBroker.host must_== broker.host
      boxBroker.port must_== broker.port
    }
  }

  "Broker" should {
    "have a reasonable toString" in {
      broker.toString must_== "id:0;host:mock.dev.box.net;port:8000"
    }

    "provide connection string" in {
      broker.connectionString must_== "mock.dev.box.net:8000"
    }
  }
}
