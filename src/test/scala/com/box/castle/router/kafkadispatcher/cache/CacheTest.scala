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
      val c = Cache(300)
      c.data must throwA(new IllegalAccessException())
      c.currentSizeInBytes must throwA(new IllegalAccessException())
    }

    "take a castle message batch as a parameter directly" in {
      val b = createBatch(40, 10, 275)
      val c = Cache(b, 20000)
      c.get(40) must_== Some(b)
    }

    "have a reasonable toString" in {
      val b = createBatch(40, 9, 467)
      val c = Cache(b, 468)
      c.toString must_== "Cache(maxSizeInBytes=468,currentSizeInBytes=467," +
        "data=LinkedHashMap(40 -> CastleMessageBatch(offset=40,nextOffset=49,size=9,sizeInBytes=467,maxOffset=48)))"
    }

    "not allow 0 sized max size in bytes" in {
      val b = createBatch(40, 10, 467)
      Cache(b, 0) must throwA(new IllegalArgumentException(
        s"requirement failed: Cache must have more than 0 bytes to use"))
    }

    "return values when partially matching a batch" in {
      var cache = Cache(800)
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

      var cache = Cache(3000)
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

      var cache = Cache(3000)
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

      var cache = Cache(3000)
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
      var cache = Cache(319 + 50 + 207)
      cache = cache.add(createBatch(20, 3, 319))
      cache = cache.add(createBatch(23, 1, 50))
      cache = cache.add(createBatch(24, 5, 207))

      // Establish that all 3 batches are in the cache
      cache.get(20).get.sizeInBytes must_== 319
      cache.get(23).get.sizeInBytes must_== 50
      cache.get(24).get.sizeInBytes must_== 207

      cache.currentSizeInBytes must_== 319 + 50 + 207
      cache.currentSizeInBytes must_== cache.maxSizeInBytes

      // adding a batch with even just a 1 items in it, causes us to go over
      // and we remove the oldest message even though it is the biggest
      cache = cache.add(createBatch(34, 1, MinimumMessageSetSize))
      cache.get(20) must_== None
      cache.get(23).get.sizeInBytes must_== 50
      cache.get(24).get.sizeInBytes must_== 207
      cache.get(34).get.sizeInBytes must_== MinimumMessageSetSize

      // Now there's enough space for a message of size 319 - MessageSet.LogOverhead
      cache = cache.add(createBatch(35, 1, 319 - MinimumMessageSetSize))
      cache.get(23).get.sizeInBytes must_== 50
      cache.get(24).get.sizeInBytes must_== 207
      cache.get(34).get.sizeInBytes must_== MinimumMessageSetSize
      cache.get(35).get.sizeInBytes must_== 319 - MinimumMessageSetSize

      cache.currentSizeInBytes must_== 319 + 50 + 207
      cache.currentSizeInBytes must_== cache.maxSizeInBytes

      // Adding a message the size of the entire max batch, will kick out all the other batches
      cache = cache.add(createBatch(36, 1, 319 + 50 + 207))
      cache.get(23) must_== None
      cache.get(24) must_== None
      cache.get(34) must_== None
      cache.get(35) must_== None
      cache.get(36).get.sizeInBytes must_== 319 + 50 + 207

      // It's not possible to add a message that is too big
      cache = cache.add(createBatch(37, 1, 319 + 50 + 207 + 1))
      cache.get(36).get.sizeInBytes must_== 319 + 50 + 207
      cache.get(37) must_== None

    }

    "return values in the middle of batches even if there are gaps between batches" in {
      var cache = Cache(400)
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
      val c = Cache(MinimumMessageSetSize - 1)
      c.isInstanceOf[EmptyCache] must_== true

      val c2 = c.add(createBatch(offset=20, numMessages=1, sizeInBytes=MinimumMessageSetSize))
      c2.isInstanceOf[EmptyCache] must_== true

      c eq c2 must_== true
    }

    "properly adds a max sized message batch into an EmptyCache" in {
      val c = Cache(MinimumMessageSetSize)
      c.isInstanceOf[EmptyCache] must_== true

      val b1 = createBatch(offset=20, numMessages=1, sizeInBytes=MinimumMessageSetSize)
      val c2 = c.add(b1)
      c2.get(20) must_== Some(b1)
    }

    "returns the exact same object when adding a batch that is too big to fit into a Cache with data" in {
      val initialBatch = createBatch(offset=20, numMessages=1, sizeInBytes=MinimumMessageSetSize)
      val c = Cache(MinimumMessageSetSize).add(initialBatch)
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
      val c = Cache(75).add(initialBatch)
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
      val c = Cache(25)
      c.isInstanceOf[EmptyCache] must_== true

      val c2 = c.setMaxSizeInBytes(25)
      c2.isInstanceOf[EmptyCache] must_== true
      c eq c2 must_== true
    }

    "return the exact same object when setting the same size in bytes for cache with data" in {
      val c = Cache(300).add(createBatch(20, 1, 50))
      c.isInstanceOf[CacheWithData] must_== true

      val c2 = c.setMaxSizeInBytes(300)
      c2.isInstanceOf[CacheWithData] must_== true
      c eq c2 must_== true
    }

    "clear out oldest keys first" in {
      var cache = Cache(3000)
      val b1 = createBatch(20, 2, 65)
      val b2 = createBatch(23, 1, 87)
      val b3 = createBatch(24, 10, 912)

      cache = cache.add(b1)
      cache = cache.add(b2)
      cache = cache.add(b3)

      // Establish that all 3 batches are in the cache
      cache.get(20).get.sizeInBytes must_== 65
      cache.get(23).get.sizeInBytes must_== 87
      cache.get(24).get.sizeInBytes must_== 912

      cache = cache.setMaxSizeInBytes(65 + 87 + 912)
      cache.get(20).get.sizeInBytes must_== 65
      cache.get(23).get.sizeInBytes must_== 87
      cache.get(24).get.sizeInBytes must_== 912

      cache = cache.setMaxSizeInBytes(65 + 87 + 912 - 1)
      cache.get(20) must_== None
      cache.get(23).get.sizeInBytes must_== 87
      cache.get(24).get.sizeInBytes must_== 912

      cache = cache.setMaxSizeInBytes(87 + 912)
      cache.get(20) must_== None
      cache.get(23).get.sizeInBytes must_== 87
      cache.get(24).get.sizeInBytes must_== 912

      cache = cache.setMaxSizeInBytes(87 + 912 - 1)
      cache.get(20) must_== None
      cache.get(23) must_== None
      cache.get(24).get.sizeInBytes must_== 912

      cache = cache.setMaxSizeInBytes(912)
      cache.get(20) must_== None
      cache.get(23) must_== None
      cache.get(24).get.sizeInBytes must_== 912

      cache = cache.setMaxSizeInBytes(912 - 1)
      cache.get(20) must_== None
      cache.get(23) must_== None
      cache.get(24) must_== None


      cache = cache.setMaxSizeInBytes(65 + 87 + 912)

      cache = cache.add(b1)
      cache = cache.add(b2)
      cache = cache.add(b3)
      // Establish that all 3 batches are in the cache
      cache.get(20).get.sizeInBytes must_== 65
      cache.get(23).get.sizeInBytes must_== 87
      cache.get(24).get.sizeInBytes must_== 912

      // Now let's remove them all in one shot
      cache = cache.setMaxSizeInBytes(912 - 1)
      cache.get(20) must_== None
      cache.get(23) must_== None
      cache.get(24) must_== None
    }

    "correctly handle going to an empty cache from one with data" in {
      var cache = Cache(43)
      val b1 = createBatch(20, 1, 43)

      cache = cache.add(b1)
      // Verify its in there
      cache.get(20).get.sizeInBytes must_== 43

      cache = cache.setMaxSizeInBytes(12)
      // Verify its not in there
      cache.get(20) must_== None

    }
  }
}