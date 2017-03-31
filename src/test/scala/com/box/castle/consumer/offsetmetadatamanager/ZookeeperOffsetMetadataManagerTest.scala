package com.box.castle.consumer.offsetmetadatamanager

import com.box.castle.consumer.offsetmetadatamanager.ZookeeperOffsetMetadataManager.ZookeeperPayload
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.api._
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api.{CreateBuilder, ExistsBuilder}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.specs2.mock.Mockito

import scala.concurrent.duration.Duration
//import org.specs2.mock.Mockito._
import org.specs2.mutable.Specification
import org.apache.zookeeper.data.Stat
import java.nio.file.{Path, Paths}
import java.util.concurrent.TimeUnit._

import kafka.api.{OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse}
import kafka.common.{OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer.SimpleConsumer

import scala.language.reflectiveCalls
import scala.util.Random
import scala.util.control.Exception

import org.apache.curator.RetryPolicy
import org.apache.curator.retry.ExponentialBackoffRetry

import scala.concurrent.duration.{FiniteDuration, Duration}

class ZookeeperOffsetMetadataManagerTest extends Specification with Mockito {

  class TestZookeeperOffsetMetadataManager
    extends ZookeeperOffsetMetadataManager(Paths.get(""), "", new ExponentialBackoffRetry(1500, 3),
      FiniteDuration(10, SECONDS), FiniteDuration(30, SECONDS), FiniteDuration(30, SECONDS)) {

    def createExistsBuilder = mock[ExistsBuilder]

    curator = {
      val mockCurator = mock[CuratorFramework]
      mockCurator.checkExists() returns createExistsBuilder

      mockCurator
    }

    override def recreateCuratorWithDelay(delay: Boolean = true): Unit = {}

    override def createCurator(): CuratorFramework = curator
  }

  "ZookeeperOffsetMetadataManager" should {
    "properly check for existence of existing ZK Path" in {
      val existingPath = "/existing/path"

      val zkOffsetMetadataManager = new TestZookeeperOffsetMetadataManager {
        override def createExistsBuilder = {
          val mockExistsBuilder = mock[ExistsBuilder]
          mockExistsBuilder.forPath(existingPath) returns mock[Stat]
          mockExistsBuilder
        }
      }

      zkOffsetMetadataManager.exists(zkOffsetMetadataManager.curator, existingPath) mustEqual true

    }

    "properly check for existence of non-existing ZK Path" in {
      val existingPath = "/non_existing/path"

      val zkOffsetMetadataManager = new TestZookeeperOffsetMetadataManager {
        override def createExistsBuilder = {
          val mockExistsBuilder = mock[ExistsBuilder]
          mockExistsBuilder.forPath(existingPath) returns null
          mockExistsBuilder
        }
      }

      zkOffsetMetadataManager.exists(zkOffsetMetadataManager.curator, existingPath) mustEqual false

    }

    "JSON serialize Zookeeper Payload" in {
      val zkPayload = ZookeeperPayload(123, """{hello: 1, world: 2}""", 0, 1, "client_id")
      val expectedJson = """{"offset":123,"metadata":"{hello: 1, world: 2}","error":0,"correlation_id":1,"client_id":"client_id"}"""

      val zkOffsetMetadataManager = new TestZookeeperOffsetMetadataManager

      zkOffsetMetadataManager.formatZkPayload(zkPayload) mustEqual expectedJson
    }

    "Deserialize JSON to Zookeeper Payload" in {
      val zkPayload = ZookeeperPayload(123, """{hello: 1, world: 2}""", 0, 1, "client_id")
      val expectedJson = """{"offset":123,"metadata":"{hello: 1, world: 2}","error":0,"correlation_id":1,"client_id":"client_id"}"""

      val zkOffsetMetadataManager = new TestZookeeperOffsetMetadataManager
      zkOffsetMetadataManager.parseZkPayload(expectedJson) mustEqual zkPayload
    }

    "properly issue a Zk Put on a path that does not exist" in {
      val testPayload = """{"offset":123,"metadata":"{hello: 1, world: 2}","error":0,"correlation_id":1,"client_id":"client_id"}"""
      val rawStringBytes = testPayload.getBytes("UTF-8")
      val zkPath = "consumers"

      val zkOffsetMetadataManager = new TestZookeeperOffsetMetadataManager {
        override def exists(c: CuratorFramework, path: String): Boolean = false

        val protectACLBytesable = mock[ProtectACLCreateModePathAndBytesable[String]]

        curator = {
          val createBuilder = mock[CreateBuilder]
          createBuilder.creatingParentsIfNeeded() returns protectACLBytesable

          val mockCurator = mock[CuratorFramework]
          mockCurator.create() returns createBuilder

          mockCurator
        }
      }

      zkOffsetMetadataManager.put(Paths.get(zkPath), testPayload) mustEqual OffsetMetadataManagerErrorCodes.NoError
      there was one(zkOffsetMetadataManager.protectACLBytesable).forPath(zkPath, rawStringBytes)
    }

    "properly issue a Zk Put on a path that exists" in {
      val testPayload = """{"offset":123,"metadata":"{hello: 1, world: 2}","error":0,"correlation_id":1,"client_id":"client_id"}"""
      val rawStringBytes = testPayload.getBytes("UTF-8")
      val zkPath = "consumers"

      val zkOffsetMetadataManager = new TestZookeeperOffsetMetadataManager {
        override def exists(c: CuratorFramework, path: String): Boolean = true
        val setDataBuilder = mock[SetDataBuilder]

        curator = {
          val mockCurator = mock[CuratorFramework]
          mockCurator.setData() returns setDataBuilder

          mockCurator
        }
      }

      zkOffsetMetadataManager.put(Paths.get(zkPath), testPayload) mustEqual OffsetMetadataManagerErrorCodes.NoError
      there was one(zkOffsetMetadataManager.setDataBuilder).forPath(zkPath, rawStringBytes)
    }

    "handle Zk Put size too large by returning the correct error code" in {
      val testPayload = new StringBuilder(2000000); //generate 2MB payload
      val paddingString = "payloadpayloadpayloadpayloadpayloadpayloadpayload"

      while (testPayload.length() + paddingString.length() < 2000000)
        testPayload.append(paddingString)
      val zkPath = "consumers"

      val zkOffsetMetadataManager = new TestZookeeperOffsetMetadataManager {
        override def exists(c: CuratorFramework, path: String): Boolean = true

        curator = mock[CuratorFramework]
      }

      zkOffsetMetadataManager.put(Paths.get(zkPath), testPayload.toString) mustEqual OffsetMetadataManagerErrorCodes.OffsetMetadataValueTooLarge
    }

    "correctly return value of Znode on existing Znode upon Zk Get" in {
      val testPayload = """{"offset":123,"metadata":"{hello: 1, world: 2}","error":0,"correlation_id":1,"client_id":"client_id"}"""
      val rawStringBytes = testPayload.getBytes("UTF-8")
      val zkPath = "consumers"

      val zkOffsetMetadataManager = new TestZookeeperOffsetMetadataManager {

        curator = {
          val getDataBuilder = mock[GetDataBuilder]
          getDataBuilder.forPath(zkPath) returns rawStringBytes

          val mockCurator = mock[CuratorFramework]
          mockCurator.getData() returns getDataBuilder

          mockCurator
        }
      }

      zkOffsetMetadataManager.get(Paths.get(zkPath)) mustEqual (testPayload, OffsetMetadataManagerErrorCodes.NoError)
    }

    "commit offsets for all topics and partitions and return proper error codes" in {
      val zkPathNoError    = Paths.get("zk_path_no_error")
      val zkPathffsetMetaError  = Paths.get("offset_meta")
      val zkPathUnknownError  = Paths.get("unknown_error")

      val topicPartition_1 = TopicAndPartition("topic", 1)
      val topicPartition_2 = TopicAndPartition("topic", 2)
      val offsetMetadata = OffsetMetadataAndError(123, "metadata", 0)

      val requestInfo = Map(topicPartition_1 -> offsetMetadata, topicPartition_2 -> offsetMetadata)

      val requestResponse = Map(topicPartition_1 -> OffsetMetadataManagerErrorCodes.NoError,
        topicPartition_2 -> OffsetMetadataManagerErrorCodes.OffsetMetadataValueTooLarge)

      val offsetCommitRequest  = OffsetCommitRequest("group_id", requestInfo, 1, 1, "client_id")
      val offsetCommitResponse = OffsetCommitResponse(requestResponse, 1)

      val zkOffsetMetadataManager = new TestZookeeperOffsetMetadataManager {
        override def exists(c: CuratorFramework, path: String): Boolean = true

        override def generateZkPath(consumerId: String, topic: String, partition: Int): Path = {
          if(partition == 1) zkPathNoError
          else if (partition == 2) zkPathffsetMetaError
          else zkPathUnknownError
        }

        override def put(zkPath: Path, value: String): Short = {
          if(zkPath == zkPathNoError) OffsetMetadataManagerErrorCodes.NoError
          else if(zkPath == zkPathffsetMetaError) OffsetMetadataManagerErrorCodes.OffsetMetadataValueTooLarge
          else OffsetMetadataManagerErrorCodes.UnknownError
        }
      }

      zkOffsetMetadataManager.commitOffsets(offsetCommitRequest) mustEqual offsetCommitResponse
    }

    "correctly get offset and metadata for all topics, successful or not " in {
      val zkPathNoError       = Paths.get("zk_path_no_error")
      val zkPathNoZnodeError  = Paths.get("no_znode_error")

      val topicPartition_1 = TopicAndPartition("topic", 1)
      val topicPartition_2 = TopicAndPartition("topic", 3)

      val offsetFetchRequest   = OffsetFetchRequest("group_id",
        List(topicPartition_1, topicPartition_2), 1, 1, "client_id")

      val offsetMetadata = OffsetMetadataAndError(123, "meta_1", 0)

      val offsetFetchResponse  = OffsetFetchResponse(
        Map(topicPartition_1 -> OffsetMetadataAndError(123, "no_error_value", OffsetMetadataManagerErrorCodes.NoError),
          topicPartition_2 -> OffsetMetadataAndError(0, "", OffsetMetadataManagerErrorCodes.ZNodeDoesNotExist)),
        1)

      val zkOffsetMetadataManager = new TestZookeeperOffsetMetadataManager {
        override def exists(c: CuratorFramework, path: String): Boolean = true

        override def generateZkPath(consumerId: String, topic: String, partition: Int): Path = {
          if(partition == 1) zkPathNoError
          else zkPathNoZnodeError
        }

        override def parseZkPayload(raw: String): ZookeeperPayload ={
          if(raw == "no_error_value")
            ZookeeperPayload(123, "no_error_value", OffsetMetadataManagerErrorCodes.NoError, 1, "client_id")
          else ZookeeperPayload(1, "unexpected", OffsetMetadataManagerErrorCodes.CannotConnectToPersistentStore, 1, "client_id")
        }

        override def get(zkPath: Path): (String, Short) = {
          if(zkPath == zkPathNoError) ("no_error_value", OffsetMetadataManagerErrorCodes.NoError)
          else if(zkPath == zkPathNoZnodeError) ("", OffsetMetadataManagerErrorCodes.ZNodeDoesNotExist)
          else ("", OffsetMetadataManagerErrorCodes.ZNodeDoesNotExist)
        }
      }

      zkOffsetMetadataManager.fetchOffsets(offsetFetchRequest) mustEqual offsetFetchResponse
    }
  }
}