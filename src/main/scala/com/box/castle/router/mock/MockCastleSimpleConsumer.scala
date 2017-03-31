package com.box.castle.router.mock

import java.util.concurrent.{Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import com.box.castle.consumer.CastleSimpleConsumer._
import com.box.castle.consumer._
import com.box.castle.consumer.offsetmetadatamanager.ZookeeperOffsetMetadataManagerFactory
import com.box.kafka.Broker
import java.nio.file.{Path, Paths}

import kafka.api._
import kafka.common.{OffsetMetadataAndError, TopicAndPartition}

import scala.collection.immutable.Queue
import scala.concurrent.duration.FiniteDuration

class MockCastleSimpleConsumer(val timeout: FiniteDuration,
                               val broker: Broker,
                               topicMetadataMockData: Map[Int, TopicMetadataResponse],
                               fetchMockData: Map[Int, Map[TopicAndPartition, Map[Long, FetchResponsePartitionData]]] = Map.empty,
                               fetchOffsetMockData: Map[Int, Map[TopicAndPartition, Map[OffsetType, PartitionOffsetsResponse]]] = Map.empty,
                               fetchConsumerOffsetMockData: Map[Int, Map[ConsumerId, Map[TopicAndPartition, OffsetMetadataAndError]]] = Map.empty,
                               commitConsumerOffsetMockData: Map[Int, Map[ConsumerId, Map[TopicAndPartition, Short]]] = Map.empty,
                               fetchSyncOption: Option[(Semaphore, Semaphore)] = None,
                               commitConsumerOffsetSyncOption: Option[(Semaphore, Semaphore)] = None,
                               fetchConsumerOffsetSyncOption: Option[(Semaphore, Semaphore)] = None,
                               useKafkaOffsetMetadataManager: Map[ConsumerId, Boolean] = Map.empty)
  extends CastleSimpleConsumer(broker, FiniteDuration(1, "ms"), 1000, ClientId("mock"),
    new SimpleConsumerFactory(), Some(new ZookeeperOffsetMetadataManagerFactory(Paths.get("base_path"), "zk_url")),
    useKafkaOffsetMetadataManager) {

  val errors = new AtomicInteger(0)
  val getTopicMetadataCalls = new AtomicInteger(0)
  var committedConsumerOffsets = Queue.empty[(ConsumerId, TopicAndPartition, OffsetMetadataAndError)]
  var topicMetadataForAllCorrelationIds: Option[Seq[TopicMetadata]] = None

  private[this] val lock = new Object()

  private def countError(): Unit = {
    errors.getAndIncrement()
  }

  def getNumErrors: Integer = {
    errors.get()
  }


  override def fetchData(requests: Map[TopicAndPartition, Long],
                         correlationId: Int = 0,
                         maxWaitTime: FiniteDuration = DefaultMaxWait,
                         minBytes: Int = DefaultMinBytes): FetchResponse = {
    fetchSyncOption.map {
      case (syncClient, syncServer) => {
        println(s"$broker mock fetchData: telling client we are processing the request")
        syncClient.release()

        println(s"$broker mock fetchData: waiting for server to acquire semaphore")
        syncServer.tryAcquire(10, TimeUnit.SECONDS)
      }
    }

    println(s"$broker -- mock fetchData: for correlationId=$correlationId, requests: $requests")
    FetchResponse(correlationId, requests.map {
      case (topicAndPartition, offset) => {
        fetchMockData.get(correlationId) match {
          case Some(byCorrelationId) =>
            byCorrelationId.get(topicAndPartition) match {
              case Some(byTopicAndPartition) =>
                byTopicAndPartition.get(offset) match {
                  case Some(response) => {
                    println(s"$broker -- mock fetchData: returning: ${fetchMockData(correlationId)(topicAndPartition)(offset)}")
                    topicAndPartition -> fetchMockData(correlationId)(topicAndPartition)(offset)
                  }
                  case None => {
                    println(s"** ERROR ** fetchMockData missing offset: $offset for " +
                      s"topicAndPartition: $topicAndPartition correlationId: $correlationId ($broker)")
                    countError()
                    throw new Exception("logic error")
                  }
                }
              case None => {
                println(s"** ERROR ** fetchMockData missing " +
                  s"topicAndPartition: $topicAndPartition for correlationId: $correlationId ($broker)")
                countError()
                throw new Exception("logic error")
              }
            }
          case None => {
            println(s"** ERROR ** fetchMockData missing correlationId: $correlationId ($broker)")
            countError()
            throw new Exception("logic error")
          }
        }
      }
    })

  }

  override def fetchTopicMetadata(correlationId: Int = 0): TopicMetadataResponse = {
    println(s"$broker fetchTopicMetadata -- fetch topic metadata called with correlationId=$correlationId")
    getTopicMetadataCalls.getAndIncrement()
    topicMetadataForAllCorrelationIds match {
      case Some(topicMetadata) => TopicMetadataResponse(topicMetadata, correlationId)
      case None => topicMetadataMockData.get(correlationId) match {
        case Some(response) => response
        case None => {
          println(s"** ERROR ** fetchTopicMetadata missing correlationId: $correlationId")
          countError()
          throw new Exception("logic error")
        }
      }
    }
  }

  override def fetchOffsets(requests: Map[TopicAndPartition, OffsetType], correlationId: Int = 0, maxNumOffsets: Int = 1): OffsetResponse = {
    OffsetResponse(correlationId, requests.map {
      case (topicAndPartition, offsetType) => {
        fetchOffsetMockData.get(correlationId) match {
          case Some(byCorrelationId) =>
            byCorrelationId.get(topicAndPartition) match {
              case Some(byTopicAndPartition) =>
                byTopicAndPartition.get(offsetType) match {
                  case Some(response) => topicAndPartition -> fetchOffsetMockData(correlationId)(topicAndPartition)(offsetType)
                  case None => {
                    println(s"** ERROR ** fetchOffsets missing offsetType: $offsetType for " +
                      s"topicAndPartition: $topicAndPartition, correlationId: $correlationId")
                    countError()
                    throw new Exception("logic error")
                  }
                }
              case None => {
                println(s"** ERROR ** fetchOffsets missing " +
                  s"topicAndPartition: $topicAndPartition for correlationId: $correlationId")
                countError()
                throw new Exception("logic error")
              }
            }
          case None => {
            println(s"** ERROR ** fetchOffsets missing correlationId: $correlationId")
            countError()
            throw new Exception("logic error")
          }
        }
      }
    })
  }

  override def fetchConsumerOffsets(consumerId: ConsumerId,
                                    topicAndPartitions: scala.collection.Seq[TopicAndPartition],
                                    correlationId: Int = 0): OffsetFetchResponse = {
    fetchConsumerOffsetSyncOption.map {
      case (syncClient, syncServer) => {
        println(s"$broker mock fetchConsumerOffsets: CHECKING WITH SYNC for consumerId=$consumerId, correlationId=$correlationId, " +
          s"topicAndPartitions=$topicAndPartitions")
        syncClient.release()
        syncServer.tryAcquire(10, TimeUnit.SECONDS)
      }
    }

    OffsetFetchResponse(topicAndPartitions.map(topicAndPartition => {
      fetchConsumerOffsetMockData.get(correlationId) match {
        case Some(byCorrelationId) => {
          byCorrelationId.get(consumerId) match {
            case Some(byConsumerId) => {
              byConsumerId.get(topicAndPartition) match {
                case Some(offsetMetadata) => {
                  (topicAndPartition, offsetMetadata)
                }
                case None => {
                  println(s"** ERROR ** fetchConsumerOffset missing topicAndPartition: $topicAndPartition for " +
                    s"consumerId: $consumerId correlationId: $correlationId")
                  countError()
                  throw new Exception("logic error")
                }
              }
            }
            case None => {
              println(s"** ERROR ** fetchConsumerOffset missing consumerId: $consumerId for correlationId: $correlationId")
              countError()
              throw new Exception("logic error")
            }
          }
        }
        case None => {
          println(s"** ERROR ** fetchConsumerOffset missing correlationId: $correlationId")
          countError()
          throw new Exception("logic error")
        }
      }
    }).toMap, correlationId)
  }


  override def commitConsumerOffsetAndMetadata(consumerId: ConsumerId,
                                    topicAndPartitionToOffsetMap: Map[TopicAndPartition, OffsetMetadataAndError],
                                    correlationId: Int = 0): OffsetCommitResponse = {
    lock.synchronized(
      topicAndPartitionToOffsetMap.foreach {
        case (topicAndPartition, offsetMetadataAndError) =>
          committedConsumerOffsets = committedConsumerOffsets.enqueue((consumerId, topicAndPartition, offsetMetadataAndError))
      }
    )

    commitConsumerOffsetSyncOption.map {
      case (syncClient, syncServer) => {
        println(s"$broker mock commitConsumerOffset: CHECKING WITH SYNC for consumerId=$consumerId, correlationId=$correlationId, " +
          s"topicAndPartitionToOffsetMap=$topicAndPartitionToOffsetMap")

        syncClient.release()
        syncServer.tryAcquire(10, TimeUnit.SECONDS)
      }
    }
    println(s"$broker mock commitConsumerOffset called with consumerId=$consumerId, correlationId=$correlationId, " +
      s"topicAndPartitionToOffsetMap=$topicAndPartitionToOffsetMap")

    OffsetCommitResponse(topicAndPartitionToOffsetMap.keys.map(topicAndPartition => {
      commitConsumerOffsetMockData.get(correlationId) match {
        case Some(byCorrelationId) => {
          byCorrelationId.get(consumerId) match {
            case Some(byConsumerId) => {
              byConsumerId.get(topicAndPartition) match {
                case Some(errorCode) => {
                  (topicAndPartition, errorCode)
                }
                case None => {
                  println(s"** ERROR ** commitConsumerOffset missing topicAndPartition: $topicAndPartition for " +
                    s"consumerId: $consumerId correlationId: $correlationId")
                  countError()
                  throw new Exception("logic error")
                }
              }
            }
            case None => {
              println(s"** ERROR ** commitConsumerOffset missing consumerId: $consumerId for correlationId: $correlationId")
              countError()
              throw new Exception("logic error")
            }
          }
        }
        case None => {
          println(s"** ERROR ** commitConsumerOffset missing correlationId: $correlationId")
          countError()
          throw new Exception("logic error")
        }
      }
    }).toMap, correlationId)
  }
}
