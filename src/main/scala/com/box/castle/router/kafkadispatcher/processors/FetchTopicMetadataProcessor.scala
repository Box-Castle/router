package com.box.castle.router.kafkadispatcher.processors

import akka.actor.Scheduler
import com.box.castle.router.exceptions.RouterFatalException
import com.box.castle.router.kafkadispatcher.KafkaDispatcherRef
import com.box.castle.router.kafkadispatcher.messages.{DispatchFetchTopicMetadataToKafka, InternalTopicMetadataResponse}
import com.box.castle.router.messages.FetchTopicMetadata
import com.box.castle.retry.strategies.TruncatedBinaryExponentialBackoffStrategy
import com.box.castle.consumer.{CastlePartitionMetadata, CastleSimpleConsumer}
import org.slf4s.Logging
import kafka.api.{PartitionMetadata, TopicMetadata, TopicMetadataResponse}
import kafka.common.TopicAndPartition

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext

/**
 * Created by dgrenader on 4/14/15.
 */
private[kafkadispatcher]
class FetchTopicMetadataProcessor(kafkaDispatcher: KafkaDispatcherRef,
                                  scheduler: Scheduler,
                                  consumer: CastleSimpleConsumer)
  extends QueueProcessor[DispatchFetchTopicMetadataToKafka, InternalTopicMetadataResponse](kafkaDispatcher) with Logging {

  private val DefaultRetryStrategy = TruncatedBinaryExponentialBackoffStrategy()

  private var correlationId = 0
  private var requesters = Set.empty[DispatchFetchTopicMetadataToKafka]
  private var tryNumber = 0

  def addToQueue(topicMetadataRequest: DispatchFetchTopicMetadataToKafka): Unit = {
    requesters = requesters + topicMetadataRequest
  }

  def isEmpty = requesters.isEmpty

  def processQueue()(implicit ec: ExecutionContext): Unit = {
    correlationId += 1
    async({
      // The async wrapper will retry this call in case of network problems
      consumer.fetchTopicMetadata(correlationId)
    },
    onSuccess = (response: TopicMetadataResponse) => kafkaDispatcher ! InternalTopicMetadataResponse(response))
  }

  // This retry is only used if there are issues with the message that we get back from the broker
  private def retry()(implicit ec: ExecutionContext): Unit = {
    val delay = DefaultRetryStrategy.delay(tryNumber)
    tryNumber += 1
    log.info(s"TopicMetadataProcessor retrying requests, try number: $tryNumber with a delay of: $delay")
    requesters.foreach(internalTopicMetadataRequest => scheduler.scheduleOnce(delay, kafkaDispatcher, internalTopicMetadataRequest))
    requesters = requesters.empty
  }

  private def unexpectedServerError()(implicit ec: ExecutionContext): Unit = {
    log.info(s"TopicMetadataProcessor got an 'UnknownError' code back " +
      s"from $consumer, retrying the request...")
    retry()
  }

  private def unknownTopicOrPartitionCode(): Unit = {
    throw new RouterFatalException(s"TopicMetadataProcessor does not request specific topics or partitions")
  }

  private def leaderNotAvailable()(implicit ec: ExecutionContext): Unit = {
    log.info(s"TopicMetadataProcessor received a 'leaderNotAvailable' code back from from $consumer. " +
      s"This is most likely because there is an issue with in sync replicas in Kafka.  Retrying...")
    retry()
  }

  def processResponse(castleResponse: InternalTopicMetadataResponse)(implicit ec: ExecutionContext): Unit = {
    val response = castleResponse.response
    log.info(s"Processing topic metadata response of size ${response.sizeInBytes} bytes for ${requesters.size} requester(s)")
    require(response.correlationId  == correlationId)

    processTopicsMetadata(response.topicsMetadata).map(metadata => {
      requesters.foreach(r => r.requesterInfo.ref ! FetchTopicMetadata.Success(r.requestId, metadata))
      requesters = requesters.empty
      // Reset our retry numbers for the next time we run into an actual issue
      tryNumber = 0
    })
  }

  // So our signatures are less awkward
  private type Metadata = Map[TopicAndPartition, CastlePartitionMetadata]

  @tailrec
  private def processTopicsMetadata(topicsMetadata: Iterable[TopicMetadata],
                                    metadata: Metadata = Map.empty)(implicit ec: ExecutionContext): Option[Metadata] = {

    topicsMetadata.headOption match {
      case Some(topicMetadata) => {
        checkErrorCode(topicMetadata.errorCode,
          noError = {
            Some(topicMetadata.partitionsMetadata)
          },
          unexpectedServerError = {
            unexpectedServerError()
            None
          },
          unknownTopicOrPartitionCode = {
            unknownTopicOrPartitionCode()
            None
          },
          leaderNotAvailable = {
            leaderNotAvailable()
            None
          },
          invalidMessageCode = {
            log.error(s"FetchTopicMetadataProcessor encountered an InvalidMessageCode while fetching topic metadata " +
              s"from ${consumer.brokerInfo} for topic: ${topicMetadata.topic}, retrying...")
            retry()
            None
          }
        ) match {
          case Some(partitionsMetadata) => {
            processPartitionsMetadata(partitionsMetadata,
              metadata,
              topicMetadata.topic) match {
              case Some(result) => processTopicsMetadata(topicsMetadata.tail, result)
              case None => None // Can't flatMap here or @tailrec gets angry
            }
          }
          case None => None // Can't flatMap here or @tailrec gets angry
        }
      }
      case None => Some(metadata)
    }
  }

  @tailrec
  private def processPartitionsMetadata(partitionsMetadata: Iterable[PartitionMetadata],
                                        metadata: Metadata,
                                        topicName: String)(implicit ec: ExecutionContext): Option[Metadata] = {
    partitionsMetadata.headOption match {
      case Some(pm) => {
        checkErrorCode(pm.errorCode,
          noError = {
            Some(CastlePartitionMetadata(pm))
          },
          unexpectedServerError = {
            unexpectedServerError()
            None
          },
          unknownTopicOrPartitionCode = {
            unknownTopicOrPartitionCode()
            None
          },
          leaderNotAvailable = {
            leaderNotAvailable()
            None
          },
          invalidMessageCode = {
            log.error(s"FetchTopicMetadataProcessor encountered an InvalidMessageCode while fetching topic metadata " +
              s"from ${consumer.brokerInfo} for topic: $topicName, partition: ${pm.partitionId}, retrying...")
            retry()
            None
          }
        ) match {
          case Some(boxPartitionMetadata) => {
            processPartitionsMetadata(partitionsMetadata.tail,
              metadata + (TopicAndPartition(topicName, pm.partitionId) -> boxPartitionMetadata),
              topicName)
          }
          case None => None // Can't flatMap here or @tailrec gets angry
        }
      }
      case None => {
        Some(metadata)
      }
    }
  }
}