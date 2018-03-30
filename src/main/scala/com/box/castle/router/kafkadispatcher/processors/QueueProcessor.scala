package com.box.castle.router.kafkadispatcher.processors

import java.util.concurrent.TimeUnit

import com.box.castle.consumer.CastleSimpleConsumer
import com.box.castle.consumer.offsetmetadatamanager.OffsetMetadataManagerErrorCodes
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.exceptions.RouterFatalException
import com.box.castle.router.kafkadispatcher.KafkaDispatcherRef
import com.box.castle.router.kafkadispatcher.messages.{DispatchToKafka, KafkaBrokerUnreachable, KafkaResponse, UnexpectedFailure, UnknownTopicPartition}
import com.box.castle.router.messages.{RefreshBrokersAndLeaders, RouterResult}
import com.box.castle.retry.RetryableFuture
import com.box.castle.retry.strategies.TruncatedBinaryExponentialBackoffStrategy
import com.box.castle.router.metrics.{TagNames, Components, Metrics}
import org.slf4s.Logging
import kafka.common.{TopicAndPartition, ErrorMapping}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}




abstract class QueueProcessor[T <: DispatchToKafka, T2 <: KafkaResponse]
    (kafkaDispatcher: KafkaDispatcherRef,
     consumer: CastleSimpleConsumer,
     metricsLogger: MetricsLogger) extends Logging {

  // TODO: Make configurable, maybe
  // We will on average wait about 2 minutes before giving up with this retry strategy
  private val retryStrategy = TruncatedBinaryExponentialBackoffStrategy(
                                  slotSize = FiniteDuration(117, TimeUnit.MILLISECONDS),
                                  maxSlots = 10,
                                  maxRetries = 10)

  private val preRetry = (numRetries: Int, delay: FiniteDuration, cause: Throwable) => {
    log.warn(s"Retrying async request number $numRetries to broker " +
      s"due to an exception in ${this.getClass.getSimpleName}.  Next try in $delay. Error: ${cause.getMessage}")
  }

  def isEmpty: Boolean

  def processQueue()(implicit ec: ExecutionContext): Unit

  def getFromCache(request: T): Option[RouterResult] = None

  def addToQueue(request: T): Unit

  def processResponse(response: T2)(implicit ec: ExecutionContext): Unit

  def count(metricName: String, value: Long = 1): Unit = {
    metricsLogger.count(Components.KafkaDispatcher,
      metricName,
      Map(TagNames.BrokerHost -> consumer.host),
      value)
  }

  def handleUnknownTopicOrPartitionCode(topicAndPartition: TopicAndPartition): Unit = {
    log.error(s"${this.getClass.getSimpleName} encountered an UnknownTopicOrPartitionCode while " +
      s"interacting with this broker: ${consumer.brokerInfo} using topic and partition: $topicAndPartition. " +
      s"This is normal if the replica changes are happening in Kafka, however if you continue to see this message " +
      s"repeatedly, it may mean one of the clients of the Router is mis-configured and is attempting to get data for " +
      s"a topic and partition that does not exist.  Refreshing brokers and leaders.")
    kafkaDispatcher ! UnknownTopicPartition(topicAndPartition)
    count(Metrics.UnknownTopicOrPartition)
  }

  protected def async[R](body: => R, onSuccess: (R) => Unit)(implicit ec: ExecutionContext): Unit = {
    RetryableFuture({
      body
    }, preRetry, retryStrategy) onComplete {
      case Success(response) => {
        try {
          onSuccess(response)
        }
        catch {
          case t: Throwable => kafkaDispatcher ! UnexpectedFailure(t)
        }
      }
      case Failure(t) => {
        kafkaDispatcher ! KafkaBrokerUnreachable(t)
      }
    }
  }

  /**
   * Returns True if we found an error, False otherwise.
   */
  protected def checkErrorCode[ReturnType](
       errorCode: Short,
       noError: => ReturnType,
       unexpectedServerError: => ReturnType,
       unknownTopicOrPartitionCode: => ReturnType,
       leaderNotAvailable: => ReturnType,
       invalidMessageCode: => ReturnType,
       offsetOutOfRange: => ReturnType = {
         throw new RouterFatalException(s"${this.getClass.getSimpleName} does not fetch messages")
       },
       offsetMetadataTooLarge: => ReturnType = {
         throw new RouterFatalException(s"${this.getClass.getSimpleName} does not commit offset metadata")
       }): ReturnType = {

    errorCode match {
      // 0 - No error--it worked!
      case ErrorMapping.NoError => noError

      // -1 - An unexpected server error
      case ErrorMapping.UnknownCode => unexpectedServerError

      // 1 - The requested offset is outside the range of offsets maintained by the server for the given topic/partition.
      case ErrorMapping.OffsetOutOfRangeCode => offsetOutOfRange

      // 2 - This indicates that a message contents does not match its CRC
      case ErrorMapping.InvalidMessageCode => invalidMessageCode

      // 3 - This request is for a topic or partition that does not exist on this broker.
      case ErrorMapping.UnknownTopicOrPartitionCode => unknownTopicOrPartitionCode

      // 4 - The message has a negative size
      case ErrorMapping.InvalidFetchSizeCode => {
        // The name of the constant is a bit strange, it seems to indicate that the fetch size is wrong
        // but the documentation indicates that a negative size was specified for the message
        // which would only happen if we were producing messages?  This one is not really clear to me.
        throw new RouterFatalException(s"${this.getClass.getSimpleName} does not produce messages")
      }

      // 5 - This error is thrown if we are in the middle of a leadership election and there is currently
      //     no leader for this partition and hence it is unavailable for writes.
      // 6 - This error is thrown if the client attempts to send messages to a replica that is not the
      //     leader for some partition. It indicates that the clients metadata is out of date.
      case ErrorMapping.LeaderNotAvailableCode |
           ErrorMapping.NotLeaderForPartitionCode => {
        leaderNotAvailable
      }

      // 7 - This error is thrown if the request exceeds the user-specified time limit in the request.
      case ErrorMapping.RequestTimedOutCode => {
        // This will probably only apply to the message fetcher
        throw new RouterFatalException(s"${this.getClass.getSimpleName} does not specify a min bytes to fetch so " +
          "it is not possible for it to receive a request timed out error code")
      }

      // 8 - This is not a client facing error and is used mostly by tools when a broker is not alive.
      case ErrorMapping.BrokerNotAvailableCode => {
        throw new RouterFatalException(
          "This is not a client facing error and is used mostly by tools when a broker is not alive.")
      }

      // 9 - If replica is expected on a broker, but is not (this can be safely ignored).
      case ErrorMapping.ReplicaNotAvailableCode => noError  // Ignoring error per the documentation

      // 10 - The server has a configurable maximum message size to avoid unbounded memory allocation.
      //      This error is thrown if the client attempt to produce a message larger than this maximum.
      case ErrorMapping.MessageSizeTooLargeCode => {
        throw new RouterFatalException(s"${this.getClass.getSimpleName} does not produce data")
      }

      // 11 - Internal error code for broker-to-broker communication.
      case ErrorMapping.StaleControllerEpochCode => {
        throw new RouterFatalException("Internal error code for broker-to-broker communication.")
      }

      // 12 - If you specify a string larger than configured maximum for offset metadata
      case ErrorMapping.OffsetMetadataTooLargeCode => offsetMetadataTooLarge

      // 13 - Undocumented on https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
      case ErrorMapping.StaleLeaderEpochCode => {
        throw new RouterFatalException(s"Undocumented error code: " + ErrorMapping.StaleLeaderEpochCode)
      }

      // 20100 - Cannot connect to OffsetMetadataManager persistent store
      case OffsetMetadataManagerErrorCodes.CannotConnectToPersistentStore => {
        throw new RouterFatalException(s"Cannot connect to OffsetMetadataManager persistent store")
      }

      // 20200 - Invalid Offset Metadata Key, if persistent store is Zk, ensure the path is valid
      case OffsetMetadataManagerErrorCodes.InvalidOffsetMetadataKey => {
        throw new RouterFatalException(
          s"Invalid OffsetMetadataManager persistent store key. If Zookeeper is used ensure valid Zookeeper Path")
      }

      // 20300 - OffsetMetadata Value size too large
      case OffsetMetadataManagerErrorCodes.OffsetMetadataValueTooLarge => {
        throw new RouterFatalException(s"OffsetMetadata size too large")
      }

      // 20400 - ZNode Does not Exist
      case OffsetMetadataManagerErrorCodes.ZNodeDoesNotExist => unknownTopicOrPartitionCode

      case errorCode => throw new RouterFatalException(s"Unrecognized error code {errorCode}")
    }
  }
}