package com.box.castle.router.kafkadispatcher.cache

import com.box.castle.router.mock.MockBatchTools
import kafka.common.TopicAndPartition
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification




class FetchDataProcessorCacheTest extends Specification with Mockito with MockBatchTools {

  "DataFetchProcessorCache" should {

    "have a reasonable toString" in {
      val topicAndPartition = TopicAndPartition("perf", 1)
      val initialBatch = createBatch(20, 3, sizeInBytes=290)
      val c = FetchDataProcessorCache(290).add(topicAndPartition, initialBatch)
      c.toString must_== "Map([perf,1] -> Cache(maxSizeInBytes=290,currentSizeInBytes=290," +
        "data=LinkedHashMap(20 -> CastleMessageBatch(offset=20,nextOffset=23,size=3,sizeInBytes=290,maxOffset=22))))"
    }

    "not allow 0 sized max size in bytes" in {
      FetchDataProcessorCache(0) must throwA(new IllegalArgumentException(
        s"requirement failed: Cache must have more than 0 bytes to use"))

      FetchDataProcessorCache(-1) must throwA(new IllegalArgumentException(
        s"requirement failed: Cache must have more than 0 bytes to use"))
    }

    "correctly resize empty DataFetchProcessorCache" in {
      val topicAndPartition = TopicAndPartition("perf", 1)
      val maxSize = 500
      val c = FetchDataProcessorCache(maxSize)
      val b = createBatch(20, 3, sizeInBytes=maxSize)
      // Verify the original one has enough room for this
      c.add(topicAndPartition, b).get(topicAndPartition, 20) must_== Some(b)

      // The resized one does not have enough room
      c.setMaxSizeInBytes(maxSize - 1).add(topicAndPartition, b).get(topicAndPartition, 20) must_== None


    }

    "return the exact same object when setting the same size in bytes for cache with data" in {
      val c = FetchDataProcessorCache(300)
      val c2 = c.setMaxSizeInBytes(300)
      c eq c2 must_== true
    }

    "return an empty DataFetchProcessorCache cache when kicking out the last batch on a resize" in {
      val topicAndPartition = TopicAndPartition("perf", 1)
      val maxSize = 500
      val initialBatch = createBatch(20, 3, sizeInBytes=maxSize)
      val c = FetchDataProcessorCache(maxSize).add(topicAndPartition, initialBatch)

      c.get(topicAndPartition, 20) must_== Some(initialBatch)

      val c2 = c.setMaxSizeInBytes(maxSize - 1)
      c2.get(topicAndPartition, 20) must_== None
    }

    "allow a single topic and partition to take all the space if it's the only one there"  in {
      val topicAndPartition = TopicAndPartition("perf", 1)
      val cache = FetchDataProcessorCache(57 + 79 + 219)
        .add(topicAndPartition, createBatch(20, 2, 57))
        .add(topicAndPartition, createBatch(23, 1, 79))
        .add(topicAndPartition, createBatch(24, 7, 219))

      // Establish that all 3 batches are in the cache
      cache.get(topicAndPartition, 20).get.sizeInBytes must_== 57
      cache.get(topicAndPartition, 23).get.sizeInBytes must_== 79
      cache.get(topicAndPartition, 24).get.sizeInBytes must_== 219
    }

    "return None if either the topic and partition or the offset is not there"  in {
      val topicAndPartition = TopicAndPartition("perf", 1)
      val cache = FetchDataProcessorCache(160 + 120 + 419)
        .add(topicAndPartition, createBatch(20, 3, 160))
        .add(topicAndPartition, createBatch(23, 1, 120))
        .add(topicAndPartition, createBatch(24, 7, 419))

      cache.get(topicAndPartition, 2355) must_== None
      cache.get(TopicAndPartition("x", 9000), 20) must_== None

    }

    "split the space evenly when adding more topics"  in {
      val topicAndPartition = TopicAndPartition("perf", 1)
      var cache = FetchDataProcessorCache(160 + 120 + 40)
        .add(topicAndPartition, createBatch(20, 3, 160))
        .add(topicAndPartition, createBatch(23, 1, 120))
        .add(topicAndPartition, createBatch(24, 1, 40))

      // Establish that all 3 batches are in the cache
      cache.get(topicAndPartition, 20).get.sizeInBytes must_== 160
      cache.get(topicAndPartition, 23).get.sizeInBytes must_== 120
      cache.get(topicAndPartition, 24).get.sizeInBytes must_== 40

      val topicAndPartition2 = TopicAndPartition("perf", 2)
      cache = cache.add(topicAndPartition2, createBatch(18, 4, 160))

      // Now the max for each is 160
      cache.get(topicAndPartition, 20) must_== None
      cache.get(topicAndPartition, 23).get.sizeInBytes must_== 120
      cache.get(topicAndPartition, 24).get.sizeInBytes must_== 40

      cache.get(topicAndPartition2, 18).get.sizeInBytes must_== 160


      val topicAndPartition3 = TopicAndPartition("perf", 3)
      cache = cache.add(topicAndPartition3, createBatch(38, 2, 70))

      // Now the max for each is 100
      cache.get(topicAndPartition, 20) must_== None
      cache.get(topicAndPartition, 23) must_== None
      cache.get(topicAndPartition, 24).get.sizeInBytes must_== 40

      // No longer fits into 100
      cache.get(topicAndPartition2, 18) must_== None

      cache.get(topicAndPartition3, 38).get.sizeInBytes must_== 70

      // Ensure we replace properly for existing partitions
      cache = cache
        .add(topicAndPartition, createBatch(323, 3, 100))
        .add(topicAndPartition2, createBatch(521, 3, 100))
        .add(topicAndPartition3, createBatch(256, 3, 100))

      cache.get(topicAndPartition, 20) must_== None
      cache.get(topicAndPartition, 23) must_== None
      cache.get(topicAndPartition, 24) must_== None
      cache.get(topicAndPartition, 323).get.sizeInBytes must_== 100

      cache.get(topicAndPartition2, 18) must_== None
      cache.get(topicAndPartition2, 521).get.sizeInBytes must_== 100

      cache.get(topicAndPartition3, 38) must_== None
      cache.get(topicAndPartition3, 256).get.sizeInBytes must_== 100
    }
  }
}
