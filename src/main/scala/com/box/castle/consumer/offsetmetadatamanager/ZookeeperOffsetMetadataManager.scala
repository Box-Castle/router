package com.box.castle.consumer.offsetmetadatamanager

import com.box.castle.consumer.offsetmetadatamanager.OffsetMetadataManager.OffsetMetadataManagerInitializationException
import java.io.Closeable
import java.lang.IllegalArgumentException
import java.lang.Thread
import java.nio.file.{Path, Paths}
import java.net.BindException

import org.slf4s.Logging
import kafka.api.{OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse}
import kafka.common.{OffsetMetadataAndError, TopicAndPartition}
import org.apache.curator.{CuratorConnectionLossException, RetryPolicy}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.KeeperException.{ConnectionLossException, NoNodeException}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

import scala.concurrent.duration._
import scala.util.{Failure, Random, Success, Try}
import scala.util.control.{Exception, NonFatal}

class ZookeeperOffsetMetadataManager(rootNameSpace: Path,
                                     connectionEndpoint: String,
                                     retryPolicy: RetryPolicy,
                                     initialConnectionTimeout: Duration,
                                     connectionTimeout: Duration,
                                     sessionTimeout: Duration)
  extends OffsetMetadataManager
    with Closeable
    with Logging {

  import ZookeeperOffsetMetadataManager._

  /**
    * Determine whether znode exists
    */
  private[consumer] def exists(c: CuratorFramework, path: String): Boolean =
    Option(c.checkExists.forPath(path)).isDefined

  /**
    * Curator is used to transact with Zookeeper. This object is only spawned once in this class and reused.
    *
    * Apache curator provides the functionality of retrying the connection when timeouts (session/connection) occur
    *
    */
  private[consumer] val builder = CuratorFrameworkFactory.builder()
    .namespace(rootNameSpace.toString)
    .connectString(connectionEndpoint)
    .retryPolicy(retryPolicy)
    .connectionTimeoutMs(connectionTimeout.toMillis.toInt)
    .sessionTimeoutMs(sessionTimeout.toMillis.toInt)

  private[consumer] var curator: CuratorFramework = builder.build()

  private[consumer] def createCurator(): CuratorFramework = {
    val curator = builder.build()
    curator.start()

    if(!curator.blockUntilConnected(initialConnectionTimeout.toMillis.toInt, MILLISECONDS)){
      OffsetMetadataManagerInitializationException("Could not connect to ZK with the given timeout")
    }

    curator
  }

  private[consumer] def recreateCuratorWithDelay(delay: Boolean = true): Unit = {
    // Random.nextDouble generates value between 0.0 and 1.0
    if(delay) {
      val delayDuration = Random.nextDouble * maximumDelayTimeInMillis
      Thread.sleep(delayDuration.toLong)
    }

    Exception.ignoring(classOf[Throwable])(curator.close())
    curator = createCurator()
  }

  recreateCuratorWithDelay(false)

  /**
    * Generate ZK path given parameters
    *
    * @param consumerId
    * @param topic
    * @param partition
    * @return
    */
  private[consumer] def generateZkPath(consumerId: String, topic: String, partition: Int): Path =
    Paths.get(s"/offset_metadata/${consumerId}/${topic}/${partition.toString}")

  /**
    * Serialize ZookeeperPayload case class to string to be written to Znode. Default format is JSON
    *
    * @param zookeeperPayload
    * @return
    */
  private[consumer] def formatZkPayload(zookeeperPayload: ZookeeperPayload): String = {
    implicit val serializationFormat = Serialization.formats(NoTypeHints)
    write(zookeeperPayload)
  }

  /**
    * Deserialize string read from ZNode to ZookeeperPayload case class
    *
    * @param payload
    * @return
    */
  private[consumer] def parseZkPayload(payload: String): ZookeeperPayload = {
    implicit val deserializationFormat = DefaultFormats
    parse(payload).extract[ZookeeperPayload]
  }

  /**
    * Write Zk value to Znode specified by zkPath
    *
    * @param zkPath Zk path to write to
    * @param value Value to write to path
    * @return
    */
  private[consumer] def put(zkPath: Path, value: String): Short = {
    try {
      log.debug(s"Putting to zk with path: ${zkPath.toString} and value: ${value}")
      val rawStringBytes = value.getBytes(stringEncoding)
      if(rawStringBytes.size > znodeSizeLimit) throw ZookeeperPayloadSizeTooLarge()

      if (exists(curator, zkPath.toString)) curator.setData().forPath(zkPath.toString, rawStringBytes)
      else curator.create().creatingParentsIfNeeded().forPath(zkPath.toString, rawStringBytes)


      OffsetMetadataManagerErrorCodes.NoError
    } catch {
      //non-retryable errors
      case illegalArgumentException: IllegalArgumentException    => {
        log.error(s"Encounter IllegalArgumentException while putting with ${zkPath.toString} and value: ${value}", illegalArgumentException)
        OffsetMetadataManagerErrorCodes.InvalidOffsetMetadataKey
      }
      case payloadTooLargeException: ZookeeperPayloadSizeTooLarge => {
        log.error(s"Encounter ZookeeperPayloadSizeTooLarge while putting with ${zkPath.toString} and value: ${value}", payloadTooLargeException)
        OffsetMetadataManagerErrorCodes.OffsetMetadataValueTooLarge
      }
      //retryable errors
      case NonFatal(retryableError) => {
        retryableError match {
          case _: CuratorConnectionLossException | _: BindException | _: ConnectionLossException  => {
            log.error("Curator connection to Zookeeper lost. Retrying.")
          }
          case e: Exception => {
            log.error(s"Unknown error ${e.toString()}. Retrying.")
          }
        }
        recreateCuratorWithDelay()
        put(zkPath, value)
      }
    }
  }

  /**
    * Read Zk value in zkPath
    *
    * @param zkPath Zk path to read from
    * @return
    */
  private[consumer] def get(zkPath: Path): (String, Short) = {
    Try(curator.getData.forPath(zkPath.toString)).map(value => new String(value, stringEncoding)) match {

      case Success(rawString) => (rawString, OffsetMetadataManagerErrorCodes.NoError)
      case Failure(e) => {
        e match {
          // non-retryable exceptions
          case noNodeException: NoNodeException => {
            log.error(s"Attempting to GET non-existent ZNode Path: ${zkPath.toString}", noNodeException)
            log.error(s"This is not a fatal error if the path represents is a new topic/partition/consumer combination.")
            ("", OffsetMetadataManagerErrorCodes.ZNodeDoesNotExist)
          }
          // retryable exceptions
          case NonFatal(retryableError) => {
            retryableError match {
              case _: CuratorConnectionLossException | _: BindException | _: ConnectionLossException  => {
                log.error("Curator connection to Zookeeper lost. Retrying.")
              }
              case e: Exception => {
                log.error(s"Unknown error ${e.toString()}. Retrying.")
              }
            }
            recreateCuratorWithDelay()
            get(zkPath)
          }
        }
      }
    }
  }

  /**
    * Commit offset and metadata
    *
    * @param offsetCommitRequest
    * @return
    */
  def commitOffsets(offsetCommitRequest: OffsetCommitRequest): OffsetCommitResponse = {
    val topicAndPartitionErrorMap: Map[TopicAndPartition, Short] =
      offsetCommitRequest.requestInfo.map {
        case (topicPartition, offsetMetadataAndError) => {
          log.debug(s"Committing metadata and offset for topic: ${topicPartition.topic} partition: ${topicPartition.partition}")
          log.debug(s" with offset: ${offsetMetadataAndError.offset} and metadata: ${offsetMetadataAndError.metadata}")
          val zkPath = generateZkPath(offsetCommitRequest.groupId, topicPartition.topic, topicPartition.partition)

          val zookeeperPayload = ZookeeperPayload(offsetMetadataAndError.offset, offsetMetadataAndError.metadata,
            offsetMetadataAndError.error, offsetCommitRequest.correlationId,
            offsetCommitRequest.clientId)

          val formattedZkPayload = formatZkPayload(zookeeperPayload)

          val errorCode = put(zkPath, formattedZkPayload)

          (topicPartition, errorCode)
        }
      }

    OffsetCommitResponse(topicAndPartitionErrorMap, offsetCommitRequest.correlationId)
  }

  /**
    * Fetch offsets and metadata
    *
    * @param offsetFetchRequest
    * @return
    */
  def fetchOffsets(offsetFetchRequest: OffsetFetchRequest): OffsetFetchResponse = {
    val topicPartitionResponse: Map[TopicAndPartition, OffsetMetadataAndError] =
      Map(offsetFetchRequest.requestInfo.map{
        topicPartition => {
          log.debug(s"Fetching metadata and offset for topic: ${topicPartition.topic} partition: ${topicPartition.partition}")
          val zkPath = generateZkPath(offsetFetchRequest.groupId, topicPartition.topic, topicPartition.partition)

          val (rawZnodeContents, errorCode) = get(zkPath)

          val zookeeperPayload =
            if(rawZnodeContents.isEmpty) {
              ZookeeperPayload(0, "", OffsetMetadataManagerErrorCodes.ZNodeDoesNotExist,
                offsetFetchRequest.correlationId, offsetFetchRequest.clientId)
            }
            else parseZkPayload(rawZnodeContents)

          val offsetMetadataAndError = OffsetMetadataAndError(offset = zookeeperPayload.offset,
            metadata = zookeeperPayload.metadata, errorCode)

          (topicPartition, offsetMetadataAndError)
        }
      }: _*)

    OffsetFetchResponse(topicPartitionResponse, offsetFetchRequest.correlationId)
  }

  def close(): Unit = Exception.ignoring(classOf[Throwable])(curator.close())

}

object ZookeeperOffsetMetadataManager {
  private final val stringEncoding = "UTF-8"

  private final val znodeSizeLimit = 1024 * 1024

  private final val maximumDelayTimeInMillis = 120000

  case class ZookeeperPayloadSizeTooLarge(message: String =
                                          s"Attempting to write Zookeeper payload larger than ${znodeSizeLimit}")
    extends Exception(message)

  case class ZookeeperPayload(offset: Long, metadata: String, error: Short, correlation_id: Int, client_id: String)
}
