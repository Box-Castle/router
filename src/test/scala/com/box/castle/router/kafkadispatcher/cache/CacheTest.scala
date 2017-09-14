package com.box.castle.router.kafkadispatcher.cache

import com.box.castle.batch.CastleMessageBatch
import com.box.castle.collections.immutable.LinkedHashMap
import com.box.castle.router.mock.MockBatchTools
import kafka.message.{MessageSet, MessageAndOffset}
import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import com.box.castle.collections.immutable




class CacheTest extends Specification with Mockito with MockBatchTools {

  "Cache" should {

    "throw exceptions if trying to access members of EmptyCache" in {
      val c = Cache(300, 100)
      c.data must throwA(new IllegalAccessException())
      c.currentSizeInBytes must throwA(new IllegalAccessException())
    }

    "take a castle message batch as a parameter directly" in {
      val b = createBatch(40, 10, 275)
      val c = Cache(b, 20000, 275)
      c.get(40) must_== Some(b)
    }

    "have a reasonable toString" in {
      val b = createBatch(40, 9, 467)
      val c = Cache(b, 468, 467)
      c.toString must_== "Cache(bufferSize=467,maxSizeInBytes=468,currentSizeInBytes=467," +
        "data=LinkedHashMap(40 -> CastleMessageBatch(offset=40,nextOffset=49,size=9,sizeInBytes=467,maxOffset=48)))"
    }

    "not allow 0 sized max size in bytes" in {
      val b = createBatch(40, 10, 467)
      Cache(b, 0, 100) must throwA(new IllegalArgumentException(
        s"requirement failed: Cache must have more than 0 bytes to use"))
    }

    "not allow bufferSize = 0" in {
      val b = createBatch(40, 10, 467)
      Cache(b, 468, 0) must throwA(new IllegalArgumentException(
        s"requirement failed: Fetch bufferSize should always be more than 0 bytes"))
    }

    "return values when partially matching a batch" in {
      var cache = Cache(800, 200)
      cache = cache.add(createBatch(40, 10, 467))

      val a = makeMockMessageAndOffset(50, 51, 20)
      val b = makeMockMessageAndOffset(51, 52, 17)
      val c = makeMockMessageAndOffset(52, 53, 36)

      val messageBatch = CastleMessageBatch(makeMockMessageSet(List(a, b, c)))
      cache = cache.add(messageBatch)

      val mb2 = cache.get(50).get
      // This should be the exact object
      messageBatch eq mb2 must_== true

      mb2.offset must_== 50
      mb2.size must_== 3
      mb2.nextOffset must_== 53
      mb2.maxOffset must_== 52

      val mb3 = cache.get(51).get
      mb3.offset must_== 51
      mb3.size must_== 2
      mb3.nextOffset must_== 53
      mb3.maxOffset must_== 52

      val mb4 = cache.get(52).get
      mb4.offset must_== 52
      mb4.size must_== 1
      mb4.nextOffset must_== 53
      mb4.maxOffset must_== 52
    }

    "correctly slice messages on our behalf" in {
      val a = makeMockMessageAndOffset(50, 51, 20)
      val b = makeMockMessageAndOffset(51, 52, 17)
      val c = makeMockMessageAndOffset(52, 53, 36)
      val d = makeMockMessageAndOffset(53, 54, 1420)


      val messageBatch = CastleMessageBatch(makeMockMessageSet(List(a, b, c, d)))

      var cache = Cache(3000, 2000)
      cache = cache.add(messageBatch)

      val mb = cache.get(51).get
      mb.offset must_== 51
      mb.size must_== 3
      // This is to verify this is still a slice of the last message from our original message set
      mb.sizeInBytes must_== MessageSet.messageSetSize(List(b.message, c.message, d.message))
      mb.nextOffset must_== 54
      mb.maxOffset must_== 53

    }

    "reject batches with an offset not strictly greater than the existing max offset" in {
      val a = makeMockMessageAndOffset(50, 51, 20)
      val b = makeMockMessageAndOffset(51, 52, 17)
      val c = makeMockMessageAndOffset(52, 53, 36)

      val messageBatch = CastleMessageBatch(makeMockMessageSet(List(a, b, c)))

      var cache = Cache(3000, 200)
      cache = cache.add(messageBatch)

      val a2 = makeMockMessageAndOffset(52, 53, 18)
      val b2 = makeMockMessageAndOffset(53, 54, 52)
      val c2 = makeMockMessageAndOffset(54, 55, 42)

      val messageBatch2 = CastleMessageBatch(makeMockMessageSet(List(a2, b2, c2)))

      cache = cache.add(messageBatch2)

      val mb = cache.get(52).get
      mb.offset must_== 52
      mb.size must_== 1
      // This is to verify this is still a slice of the last message from our original message set
      mb.sizeInBytes must_== MessageSet.messageSetSize(List(c.message))
      mb.nextOffset must_== 53
      mb.maxOffset must_== 52

      // These never went in
      cache.get(53) must_== None
      cache.get(54) must_== None
    }

    "accept batches that have an offset strictly greater than existing max offset" in {
      val a = makeMockMessageAndOffset(50, 51, 20)
      val b = makeMockMessageAndOffset(51, 52, 17)
      val c = makeMockMessageAndOffset(52, 53, 36)

      var cache = Cache(3000, 50)
      cache = cache.add(CastleMessageBatch(makeMockMessageSet(List(a))))
      cache = cache.add(CastleMessageBatch(makeMockMessageSet(List(b))))
      cache = cache.add(CastleMessageBatch(makeMockMessageSet(List(c))))

      val mb2 = cache.get(50).get
      mb2.offset must_== 50
      mb2.size must_== 1
      mb2.nextOffset must_== 51
      mb2.maxOffset must_== 50

      val mb3 = cache.get(51).get
      mb3.offset must_== 51
      mb3.size must_== 1
      mb3.nextOffset must_== 52
      mb3.maxOffset must_== 51

      val mb4 = cache.get(52).get
      mb4.offset must_== 52
      mb4.size must_== 1
      mb4.nextOffset must_== 53
      mb4.maxOffset must_== 52
    }

    "obey max size by removing existing batches" in {
      var cache = Cache(319 + 50 + 207, 600)
      cache = cache.add(createBatch(20, 3, 319))
      cache = cache.add(createBatch(24, 1, 50))
      cache = cache.add(createBatch(26, 5, 207))

      // Establish that all 3 batches are in the cache
      cache.get(20).get.sizeInBytes must_== 319
      cache.get(24).get.sizeInBytes must_== 50
      cache.get(26).get.sizeInBytes must_== 207

      cache.currentSizeInBytes must_== 319 + 50 + 207
      cache.currentSizeInBytes must_== cache.maxSizeInBytes

      // adding a batch with just 1 item in it, causes us to go over
      // and we remove the oldest message even though it is the biggest
      cache = cache.add(createBatch(34, 1, MinimumMessageSetSize))
      cache.get(20) must_== None
      cache.get(24).get.sizeInBytes must_== 50
      cache.get(26).get.sizeInBytes must_== 207
      cache.get(34).get.sizeInBytes must_== MinimumMessageSetSize

      // Now there's enough space for a message of size 319 - MessageSet.LogOverhead
      cache = cache.add(createBatch(36, 1, 319 - MinimumMessageSetSize))
      cache.get(24).get.sizeInBytes must_== 50
      cache.get(26).get.sizeInBytes must_== 207
      cache.get(34).get.sizeInBytes must_== MinimumMessageSetSize
      cache.get(36).get.sizeInBytes must_== 319 - MinimumMessageSetSize

      cache.currentSizeInBytes must_== 319 + 50 + 207
      cache.currentSizeInBytes must_== cache.maxSizeInBytes

      // Adding a message the size of the entire max batch, will kick out all the other batches
      cache = cache.add(createBatch(37, 1, 319 + 50 + 207))
      cache.get(24) must_== None
      cache.get(26) must_== None
      cache.get(34) must_== None
      cache.get(36) must_== None
      cache.get(37).get.sizeInBytes must_== 319 + 50 + 207

      // It's not possible to add a message that is too big
      cache = cache.add(createBatch(38, 1, 319 + 50 + 207 + 1))
      cache.get(37).get.sizeInBytes must_== 319 + 50 + 207
      cache.get(38) must_== None

    }

    "return values in the middle of batches even if there are gaps between batches" in {
      var cache = Cache(400, 200)
      val batch1 = createBatch(40, 4, 200)
      cache = cache.add(batch1)

      val batch2 = createBatch(200, 4, 200)
      cache = cache.add(batch2)

      // This should be the exact same object
      batch1 eq cache.get(40).get must_== true

      // This should be the exact same object
      batch2 eq cache.get(200).get must_== true

      cache.get(38) must_== None
      cache.get(39) must_== None

      cache.get(40).get.offset must_== 40
      cache.get(41).get.offset must_== 41
      cache.get(42).get.offset must_== 42
      cache.get(43).get.offset must_== 43

      cache.get(44) must_== None
      cache.get(45) must_== None


      cache.get(198) must_== None
      cache.get(199) must_== None

      cache.get(200).get.offset must_== 200
      cache.get(201).get.offset must_== 201
      cache.get(202).get.offset must_== 202
      cache.get(203).get.offset must_== 203

      cache.get(204) must_== None
      cache.get(205) must_== None
    }
  }

  "Cache.add" should {
    "returns the exact same object when adding a batch that is too big to fit into an EmptyCache" in {
      val c = Cache(MinimumMessageSetSize - 1, MinimumMessageSetSize)
      c.isInstanceOf[EmptyCache] must_== true

      val c2 = c.add(createBatch(offset=20, numMessages=1, sizeInBytes=MinimumMessageSetSize))
      c2.isInstanceOf[EmptyCache] must_== true

      c eq c2 must_== true
    }

    "properly adds a max sized message batch into an EmptyCache" in {
      val c = Cache(MinimumMessageSetSize, MinimumMessageSetSize)
      c.isInstanceOf[EmptyCache] must_== true

      val b1 = createBatch(offset=20, numMessages=1, sizeInBytes=MinimumMessageSetSize)
      val c2 = c.add(b1)
      c2.get(20) must_== Some(b1)
    }

    "returns the exact same object when adding a batch that is too big to fit into a Cache with data" in {
      val initialBatch = createBatch(offset=20, numMessages=1, sizeInBytes=MinimumMessageSetSize)
      val c = Cache(MinimumMessageSetSize, MinimumMessageSetSize).add(initialBatch)
      c.isInstanceOf[CacheWithData] must_== true
      c.get(20) must_== Some(initialBatch)

      val b = createBatch(offset=30, numMessages=1, sizeInBytes=MinimumMessageSetSize + 1)
      // Ensures that we are testing the correct portion of the if check purely for size on bytes
      (b.offset > c.asInstanceOf[CacheWithData].maxOffset) must_== true

      val c2 = c.add(b)
      c2.isInstanceOf[CacheWithData] must_== true

      // It's the same object
      c eq c2 must_== true
    }
  }

  "Cache.setMaxSizeInBytes" should {
    "correctly shrink the maximum size if the current size of the Cache is already lower than the new lowest size" in {
      val initialBatch = createBatch(20, 1, sizeInBytes=45)
      val c = Cache(75, 100).add(initialBatch)
      c.isInstanceOf[CacheWithData] must_== true

      val newMaxSize = 60
      val b = createBatch(99, 1, sizeInBytes=newMaxSize + 1)
      val shrunk = c.setMaxSizeInBytes(newMaxSize)

      // The shrunk one cannot accommodate the batch
      shrunk.add(b).get(99) must_== None
      shrunk.add(b).get(20) must_== Some(initialBatch)

      // The old one can accommodate the batch
      c.add(b).get(99) must_== Some(b)
      // But it must kick out the initial batch
      c.add(b).get(20) must_== None
    }

    "return the exact same object when setting the same size in bytes for empty cache" in {
      val c = Cache(25, 25)
      c.isInstanceOf[EmptyCache] must_== true

      val c2 = c.setMaxSizeInBytes(25)
      c2.isInstanceOf[EmptyCache] must_== true
      c eq c2 must_== true
    }

    "return the exact same object when setting the same size in bytes for cache with data" in {
      val c = Cache(300, 50).add(createBatch(20, 1, 50))
      c.isInstanceOf[CacheWithData] must_== true

      val c2 = c.setMaxSizeInBytes(300)
      c2.isInstanceOf[CacheWithData] must_== true
      c eq c2 must_== true
    }

    "clear out oldest keys first" in {
      var cache = Cache(3000, 912)
      val b1 = createBatch(20, 2, 65)
      val b2 = createBatch(24, 1, 87)
      val b3 = createBatch(26, 10, 912)

      cache = cache.add(b1)
      cache = cache.add(b2)
      cache = cache.add(b3)

      // Establish that all 3 batches are in the cache
      cache.get(20).get.sizeInBytes must_== 65
      cache.get(24).get.sizeInBytes must_== 87
      cache.get(26).get.sizeInBytes must_== 912

      cache = cache.setMaxSizeInBytes(65 + 87 + 912)
      cache.get(20).get.sizeInBytes must_== 65
      cache.get(24).get.sizeInBytes must_== 87
      cache.get(26).get.sizeInBytes must_== 912

      cache = cache.setMaxSizeInBytes(65 + 87 + 912 - 1)
      cache.get(20) must_== None
      cache.get(24).get.sizeInBytes must_== 87
      cache.get(26).get.sizeInBytes must_== 912

      cache = cache.setMaxSizeInBytes(87 + 912)
      cache.get(20) must_== None
      cache.get(24).get.sizeInBytes must_== 87
      cache.get(26).get.sizeInBytes must_== 912

      cache = cache.setMaxSizeInBytes(87 + 912 - 1)
      cache.get(20) must_== None
      cache.get(24) must_== None
      cache.get(26).get.sizeInBytes must_== 912

      cache = cache.setMaxSizeInBytes(912)
      cache.get(20) must_== None
      cache.get(24) must_== None
      cache.get(26).get.sizeInBytes must_== 912

      cache = cache.setMaxSizeInBytes(912 - 1)
      cache.get(20) must_== None
      cache.get(24) must_== None
      cache.get(26) must_== None


      cache = cache.setMaxSizeInBytes(65 + 87 + 912)

      cache = cache.add(b1)
      cache = cache.add(b2)
      cache = cache.add(b3)
      // Establish that all 3 batches are in the cache
      cache.get(20).get.sizeInBytes must_== 65
      cache.get(24).get.sizeInBytes must_== 87
      cache.get(26).get.sizeInBytes must_== 912

      // Now let's remove them all in one shot
      cache = cache.setMaxSizeInBytes(912 - 1)
      cache.get(20) must_== None
      cache.get(24) must_== None
      cache.get(26) must_== None
    }

    "correctly handle going to an empty cache from one with data" in {
      var cache = Cache(43, 43)
      val b1 = createBatch(20, 1, 43)

      cache = cache.add(b1)
      // Verify its in there
      cache.get(20).get.sizeInBytes must_== 43

      cache = cache.setMaxSizeInBytes(12)
      // Verify its not in there
      cache.get(20) must_== None

    }
  }

  "Cache.setBufferSize" should {
    "set bufferSize correctly for Empty Cache" in {
      val c = Cache(100, 100)
      val c1 = c.setBufferSize(200)

      // Ensure that we got back an empty cache with updated bufferSize
      c.isInstanceOf[EmptyCache] shouldEqual true
      c1.isInstanceOf[EmptyCache] shouldEqual true
      c1.bufferSize shouldEqual 200
      c1.maxSizeInBytes shouldEqual c.maxSizeInBytes
    }

    "set bufferSize correctly for Cache with Data" in {
      val batch = createBatch(20, 3, 100)
      val c = Cache(100, 50).add(batch)
      val c1 = c.setBufferSize(100)

      // Ensure we got back a Cache with the same data but updated bufferSize
      c.isInstanceOf[CacheWithData] shouldEqual true
      c1.isInstanceOf[CacheWithData] shouldEqual true
      c1.bufferSize shouldEqual 100
      c1.currentSizeInBytes shouldEqual c.currentSizeInBytes
      c1.get(20).get shouldEqual batch

    }

    "return the exact same object when setting same bufferSize" in {
      val c = Cache(100,100)
      val c1 = c.setBufferSize(100)

      c eq c1 shouldEqual true

      val c2 = c.add(createBatch(20, 1, 100))
      val c3 = c2.setBufferSize(100)
      c2 eq c3 shouldEqual true
    }
  }

  "Cache.get" should {
    "return None if the cache is empty" in {
      val cache = Cache(100, 100)
      // Check it returns none
      cache.get(10) shouldEqual None
    }

    "return None in case of cache miss" in {
      val cache = Cache(300, 100).add(createBatch(20, 1, 50))
      // Check it returns none
      cache.get(50) shouldEqual None
    }

    "return None if bufferSize is too small to fit the batch" in {
      val cache = Cache(500, 400).add(createBatch(20, 3, 500))
      // Check it returns none
      cache.get(20) shouldEqual None
    }

    "return all available contiguous messages in the cache starting with given offset" in {

      val b1 = createBatch(20, 2, 65)
      val b2 = createBatch(b1.nextOffset, 1, 87)
      val b3 = createBatch(b2.nextOffset, 10, 912)
      // These batches should not be returned as they are not contiguous
      val b4 = createBatch(b3.nextOffset+1, 5, 200)
      val b5 = createBatch(b4.nextOffset, 2, 100)

      val cache = Cache(3000, 1500).add(b1).add(b2).add(b3).add(b4).add(b5)

      // Verify
      val batch = cache.get(20).get
      batch.size shouldEqual b1.size + b2.size + b3.size
      batch.sizeInBytes shouldEqual b1.sizeInBytes + b2.sizeInBytes + b3.sizeInBytes
      batch.offset shouldEqual b1.offset
      batch.nextOffset shouldEqual b3.nextOffset
    }

    "return all available contiguous messages from cache when start offset is in the middle of a batch" in {
      val b1 = createBatch(20, 2, 65)
      val b2 = createBatch(b1.nextOffset, 1, 87)
      val b3 = createBatch(b2.nextOffset, 10, 912)
      val b4 = createBatch(b3.nextOffset, 5, 200)
      val b5 = createBatch(b4.nextOffset, 2, 100)

      val cache = Cache(3000, 1200).add(b1).add(b2).add(b3).add(b4).add(b5)

      // Verify
      val batch = cache.get(28).get
      batch.size shouldEqual 5 + b4.size + b5.size
      batch.offset shouldEqual 28
      batch.nextOffset shouldEqual b5.nextOffset
    }

    "skip the last batch if it does not fit in the specified batchSize" in {
      val b1 = createBatch(20, 2, 65)
      val b2 = createBatch(b1.nextOffset, 1, 87)
      val b3 = createBatch(b2.nextOffset, 10, 912)
      val b4 = createBatch(b3.nextOffset, 5, 200)
      val b5 = createBatch(b4.nextOffset, 2, 100)

      var cache = Cache(3000,610).add(b1).add(b2).add(b3).add(b4).add(b5)

      // Batch b3 should not be returned
      val batch = cache.get(20).get
      batch.size shouldEqual b1.size + b2.size
      batch.offset shouldEqual b1.offset
      batch.nextOffset shouldEqual b2.nextOffset
      batch.sizeInBytes shouldEqual b1.sizeInBytes + b2.sizeInBytes

      // Batch b5 should not be returned as the bufferSize is 1 less than total size
      val newBufferSize = List(b1, b2, b3, b4, b5).foldLeft(0)(_ + _.sizeInBytes) - 1 // Off by 1
      cache = cache.setBufferSize(newBufferSize)
      val batch2 = cache.get(20).get
      batch2.size shouldEqual 18
      batch2.offset shouldEqual b1.offset
      batch2.nextOffset shouldEqual b4.nextOffset
      batch2.sizeInBytes must be_<=(newBufferSize)

    }

    "respect batchSize parameter when returning batches" in {
      // Setup Cache with 500 contiguous batches each of 1MB size
      val batches = (0 until 500).map( offset => createBatch(offset,1,1024*1024))
      val bufferSize = 16*1024*1024
      var cache = Cache(500*1024*1024, bufferSize)
      cache = batches.foldLeft(cache)((cache, batch) => cache.add(batch))

      // Ensure all batches have been added
      cache.currentSizeInBytes shouldEqual 500*1024*1024

      // Should return first 16 batches
      val batch1 = cache.get(0).get
      batch1.size shouldEqual 16
      batch1.sizeInBytes shouldEqual bufferSize
      batch1.offset shouldEqual 0
      batch1.nextOffset shouldEqual 16

      // Should return last 16 batches
      val batch2 = cache.get(484).get
      batch2.size shouldEqual 16
      batch2.sizeInBytes shouldEqual bufferSize
      batch2.offset shouldEqual 484
      batch2.nextOffset shouldEqual 500

      // Should return middle 16 batches
      val batch3 = cache.get(300).get
      batch3.size shouldEqual 16
      batch3.sizeInBytes shouldEqual bufferSize
      batch3.offset shouldEqual 300
      batch3.nextOffset shouldEqual 316

      // Should return last 15 batches
      val batch4 = cache.get(485).get
      batch4.size shouldEqual 15
      batch4.sizeInBytes shouldEqual 15*1024*1024
      batch4.offset shouldEqual 485
      batch4.nextOffset shouldEqual 500

      // Should return only the last batch
      val batch5 = cache.get(499).get
      batch5.size shouldEqual 1
      batch5.sizeInBytes shouldEqual batches.last.sizeInBytes
      batch5.offset shouldEqual batches.last.offset
      batch5.nextOffset shouldEqual batches.last.nextOffset

      // Should return 14 messages ( skipping the last batch added)
      // This is because the bufferSize is 1 less than the total required size to fit the last batch entirely.
      cache = cache.add(createBatch(500, 2, 2*1024*1024)).setBufferSize(bufferSize - 1)
      val batch6 = cache.get(486).get
      batch6.size shouldEqual 14
      batch6.sizeInBytes shouldEqual 14*1024*1024
      batch6.offset shouldEqual 486
      batch6.nextOffset shouldEqual 500

    }

  }
}