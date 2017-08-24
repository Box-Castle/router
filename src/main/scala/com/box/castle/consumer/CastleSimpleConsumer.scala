package com.box.castle.consumer

import java.util.concurrent.TimeUnit

import com.box.kafka.Broker
import com.box.castle.consumer.CastleSimpleConsumer.{DefaultMaxNumOffsets, DefaultMaxWait, DefaultMinBytes}
import com.box.castle.consumer.offsetmetadatamanager._
import kafka.api._
import kafka.common.{OffsetMetadataAndError, TopicAndPartition}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.Exception

class CastleSimpleConsumer(broker: Broker,
                           brokerTimeout: FiniteDuration,
                           bufferSize: Int,
                           clientId: ClientId,
                           simpleConsumerFactory: SimpleConsumerFactory,
                           optZookeeperOffsetMetadataManagerFactory: Option[OffsetMetadataManagerFactory],
                           useKafkaOffsetMetadataManager: Map[ConsumerId, Boolean]) {

  private val consumer =
    simpleConsumerFactory.create(broker, brokerTimeout, bufferSize, clientId)

  private lazy val zkOffsetMetadataManager: OffsetMetadataManager = {
    optZookeeperOffsetMetadataManagerFactory match {
      case Some(zookeeperOffsetMetadataManagerFactory) => zookeeperOffsetMetadataManagerFactory.create(consumer)
      case None => throw new IllegalArgumentException(s"Attempting to use zookeeperOffsetMetadata manager with the " +
        s" following consumers marked as false : ${useKafkaOffsetMetadataManager} but no " +
        s" ZookeeperOffsetMetadataManagerFactory was supplied, ")
    }
  }

  val brokerInfo = s"${consumer.host}:${consumer.port}"
  val host = consumer.host
  val port = consumer.port

  /**
    * Returns the consumer bufferSize
    * @return
    */
  def getBufferSize = bufferSize

  /**
    * Fetches messages at a given offset for the topic and partition provided in the request parameter
    *
    * @param requests - A map from topic and partition to the corresponding offset to fetch
    * @param correlationId - The server passes back whatever integer the client supplied as the correlation in the request.
    * @param maxWaitTime - The max wait time is the maximum amount of time in milliseconds to block waiting if
    *                      insufficient data is available at the time the request is issued.
    * @param minBytes - This is the minimum number of bytes of messages that must be available to give a response.
    *                 If the client sets this to 0 the server will always respond immediately, however if there
    *                 is no new data since their last request they will just get back empty message sets. If this
    *                 is set to 1, the server will respond as soon as at least one partition has at least 1 byte
    *                 of data or the specified timeout occurs. By setting higher values in combination with the
    *                 timeout the consumer can tune for throughput and trade a little additional latency for reading
    *                 only large chunks of data (e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k would
    *                 allow the server to wait up to 100ms to try to accumulate 64k of data before responding).
    * @return
    */
  def fetchData(requests: Map[TopicAndPartition, Long],
                correlationId: Int = 0,
                maxWaitTime: FiniteDuration = DefaultMaxWait,
                minBytes: Int = DefaultMinBytes): FetchResponse = {
    val partitionsToFetch = requests.mapValues(offset =>
          PartitionFetchInfo(offset, consumer.bufferSize))
    val request = new FetchRequest(correlationId,
                                   consumer.clientId,
                                   maxWaitTime.toMillis.toInt,
                                   minBytes,
                                   partitionsToFetch)
    consumer.fetch(request)
  }

  /**
    * Fetches topic metadata for all topics
    *
    * WARNING: this method does not correct for KAFKA-1367 (https://issues.apache.org/jira/browse/KAFKA-1367)
    *
    * @param correlationId - The server passes back whatever integer the client supplied as the correlation in the request.
    * @return
    */
  def fetchTopicMetadata(correlationId: Int = 0): TopicMetadataResponse = {
    val request = new TopicMetadataRequest(TopicMetadataRequest.CurrentVersion,
                                           correlationId,
                                           consumer.clientId,
                                           Nil)
    consumer.send(request)
  }

  /**
    * Fetches the latest or earliest offset for the given topic partition based on the OffsetType provided.
    *
    * @param requests - A map from topic partition to the corresponding offset type to fetch
    * @param correlationId - The server passes back whatever integer the client supplied as the correlation in the request.
    * @return
    */
  def fetchOffsets(
      requests: Map[TopicAndPartition, OffsetType],
      correlationId: Int = 0,
      maxNumOffsets: Int = DefaultMaxNumOffsets): OffsetResponse = {
    val requestInfo = requests.map {
      case (topicAndPartition, offsetType) =>
        topicAndPartition -> PartitionOffsetRequestInfo(offsetType.toLong,
                                                        maxNumOffsets)
    }

    val offsetRequest = new OffsetRequest(requestInfo,
                                          OffsetFetchRequest.CurrentVersion,
                                          correlationId,
                                          consumer.clientId)

    consumer.getOffsetsBefore(offsetRequest)
  }

  /**
    * Fetches offsets for the given Consumer for the topic and partitions specified.  This is used to maintain
    * the stream position for a unique consumer type.  Use commitConsumerOffset to write these.
    *
    * @param consumerId - the unique id of the consumer
    * @param topicAndPartitions - the topic and partitions for which to fetch offsets
    * @param correlationId - The server passes back whatever integer the client supplied as the correlation in the request.
    * @return
    */
  def fetchConsumerOffsets(consumerId: ConsumerId,
                           topicAndPartitions: Seq[TopicAndPartition],
                           correlationId: Int = 0): OffsetFetchResponse = {
    val offsetFetchRequest = new OffsetFetchRequest(
        consumerId.value,
        topicAndPartitions,
        OffsetFetchRequest.CurrentVersion,
        correlationId,
        consumer.clientId)

    if (useKafkaOffsetMetadataManager.getOrElse(consumerId, true))
      consumer.fetchOffsets(offsetFetchRequest)
    else
      zkOffsetMetadataManager.fetchOffsets(offsetFetchRequest)
  }

  /**
    * This is a convenience method for calling commitConsumerOffsetAndMetadata with offsets only.
    *
    * @param consumerId - the unique id of the consumer
    * @param topicAndPartitionToOffsetMap - a map from topic and partitions to the corresponding offset
    * @param correlationId - The server passes back whatever integer the client supplied as the correlation in the request.
    * @return
    */
  def commitConsumerOffset(
      consumerId: ConsumerId,
      topicAndPartitionToOffsetMap: Map[TopicAndPartition, Long],
      correlationId: Int = 0): OffsetCommitResponse = {

    val requestInfo = topicAndPartitionToOffsetMap.map {
      case (topicAndPartition, offset) =>
        topicAndPartition -> OffsetMetadataAndError(offset)
    }

    commitConsumerOffsetAndMetadata(consumerId, requestInfo, correlationId)
  }

  /**
   * Commit offsets and metadata for the given Consumer for the topic and partitions specified.  This is used to maintain
   * the stream position for a unique consumer type as well as any metadata this Consumer needs.
   * Use fetchConsumerOffsets to read these back.
   *
   * @param consumerId - the unique id of the consumer
   * @param requestInfo - a map from topic and partitions to the corresponding offset and metadata
   * @param correlationId - The server passes back whatever integer the client supplied as the correlation in the request.
   * @return
   */
  def commitConsumerOffsetAndMetadata(consumerId: ConsumerId,
                                      requestInfo: Map[TopicAndPartition, OffsetMetadataAndError],
                                      correlationId: Int = 0): OffsetCommitResponse = {


    val offsetCommitRequest = new OffsetCommitRequest(
      consumerId.value,
      requestInfo,
      OffsetFetchRequest.CurrentVersion,
      correlationId,
      consumer.clientId)

    if (useKafkaOffsetMetadataManager.getOrElse(consumerId, true))
      consumer.commitOffsets(offsetCommitRequest)
    else
      zkOffsetMetadataManager.commitOffsets(offsetCommitRequest)
  }

  def close() = {
    Exception.ignoring(classOf[Throwable])(zkOffsetMetadataManager.close())
    Exception.ignoring(classOf[Throwable])(consumer.close())
  }

  override def toString: String = brokerInfo
}

object CastleSimpleConsumer {
  // When minBytes is 0, this wait time is irrelevant and will be ignored and since
  // since our DefaultMinBytes is 0, we are OK pairing this with the default
  val DefaultMaxWait = FiniteDuration(0, TimeUnit.MILLISECONDS)

  // Tells the server to not wait for messages to appear before returning
  val DefaultMinBytes = 0

  val DefaultMaxNumOffsets = 1
}
