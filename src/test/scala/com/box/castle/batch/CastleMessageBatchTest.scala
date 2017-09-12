package com.box.castle.batch

import com.box.castle.router.mock.MockBatchTools
import kafka.message.MessageSet
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification


class CastleMessageBatchTest extends Specification with Mockito with MockBatchTools {

  "MessageBatch" should {
    "have a reasonable toString" in {
      val a = makeMockMessageAndOffset(50, 51, 100)

      val mockMessageSet = makeMockMessageSet(List(a))
      val messageBatch = CastleMessageBatch(mockMessageSet)
      messageBatch.toString must_== "CastleMessageBatch(offset=50,nextOffset=51,size=1,sizeInBytes=112,maxOffset=50)"
    }

    "throws an exception if there are no message during construction" in {
      CastleMessageBatch(makeMockMessageSet(List.empty)) must
        throwA(new IllegalArgumentException(s"requirement failed: CastleMessageBatch must not be empty"))
    }

    "correctly sets fields with one message" in {
      val a = makeMockMessageAndOffset(50, 51, 100)

      val mockMessageSet = makeMockMessageSet(List(a))
      val messageBatch = CastleMessageBatch(mockMessageSet)
      messageBatch.offset must_== 50
      messageBatch.nextOffset must_== 51
      messageBatch.size must_== 1
      messageBatch.sizeInBytes must_== 100 + MessageSet.LogOverhead
      messageBatch.maxOffset must_== 50
    }

    "correctly sets fields with two messages" in {
      val a = makeMockMessageAndOffset(50, 51, 100)
      val b = makeMockMessageAndOffset(51, 52, 200)

      val mockMessageSet = makeMockMessageSet(List(a, b))
      val messageBatch = CastleMessageBatch(mockMessageSet)
      messageBatch.offset must_== 50
      messageBatch.nextOffset must_== 52
      messageBatch.size must_== 2
      messageBatch.sizeInBytes must_== 100 + 200 + MessageSet.LogOverhead * 2
      messageBatch.maxOffset must_== 51
    }

    "correctly create a message batch from a batch vector " in {
      val a = makeMockMessageAndOffset(50, 51, 100)
      val b = makeMockMessageAndOffset(51, 52, 150)
      val c = makeMockMessageAndOffset(52, 53, 200)
      val mockMessageSet1 = makeMockMessageSet(List(a))
      val mockMessageSet2 = makeMockMessageSet(List(b, c))
      val mockMessageBatch1 = CastleMessageBatch(mockMessageSet1)
      val mockMessageBatch2 = CastleMessageBatch(mockMessageSet2)

      val combinedMessageBatch = CastleMessageBatch(Vector(mockMessageBatch1, mockMessageBatch2))

      combinedMessageBatch.size must_== 3
      combinedMessageBatch.sizeInBytes must_== 100 + 150 + 200 + MessageSet.LogOverhead * 3
      combinedMessageBatch.offset must_== 50
      combinedMessageBatch.nextOffset must_== 53
    }

    "not create a message batch from batch vector that is not contiguous" in {
      val a = makeMockMessageAndOffset(50, 51, 100)
      val b = makeMockMessageAndOffset(53, 54, 150)
      val c = makeMockMessageAndOffset(54, 55, 200)
      val mockMessageSet1 = makeMockMessageSet(List(a))
      val mockMessageSet2 = makeMockMessageSet(List(b, c))
      val mockMessageBatch1 = CastleMessageBatch(mockMessageSet1)
      val mockMessageBatch2 = CastleMessageBatch(mockMessageSet2)

      CastleMessageBatch(Vector(mockMessageBatch1, mockMessageBatch2)) should throwA(new IllegalArgumentException(s"requirement failed: Batches should be contiguous."))

    }
  }

  "MessageBatch.createBatchFromOffset" should {
    "return None for an offset not in the batch" in {
      val a = makeMockMessageAndOffset(50, 51, 100)
      val b = makeMockMessageAndOffset(51, 52, 150)
      val c = makeMockMessageAndOffset(52, 53, 200)

      val mockMessageSet = makeMockMessageSet(List(a, b, c))
      val messageBatch = CastleMessageBatch(mockMessageSet)

      messageBatch.createBatchFromOffset(-1) must_== None
      messageBatch.createBatchFromOffset(0) must_== None
      messageBatch.createBatchFromOffset(5) must_== None
      messageBatch.createBatchFromOffset(49) must_== None
      messageBatch.createBatchFromOffset(53) must_== None
    }

    "uses original size in bytes when not slicing the array" in {
      val a = makeMockMessageAndOffset(50, 51, 100)
      val b = makeMockMessageAndOffset(51, 52, 150)
      val c = makeMockMessageAndOffset(52, 53, 200)

      val mockMessageSet = makeMockMessageSet(List(a, b, c))
      val messageBatch = CastleMessageBatch(mockMessageSet)

      messageBatch.createBatchFromOffset(50).get.sizeInBytes must_== 100 + 150 + 200 + MessageSet.LogOverhead * 3
    }

    "sums up the individual message sizes when slicing" in {
      val a = makeMockMessageAndOffset(50, 51, 100)
      val b = makeMockMessageAndOffset(51, 52, 152)
      val c = makeMockMessageAndOffset(52, 53, 211)

      val mockMessageSet = makeMockMessageSet(List(a, b, c))
      val messageBatch = CastleMessageBatch(mockMessageSet)

      val m = messageBatch.createBatchFromOffset(51).get
      m.sizeInBytes must_== 152 + 211 + MessageSet.LogOverhead * 2
    }

    "return subsequences from a given offset" in {
      val a = makeMockMessageAndOffset(50, 51, 100)
      val b = makeMockMessageAndOffset(51, 52, 150)
      val c = makeMockMessageAndOffset(52, 53, 200)

      val mockMessageSet = makeMockMessageSet(List(a, b, c))
      val messageBatch = CastleMessageBatch(mockMessageSet)

      messageBatch.offset must_== 50
      messageBatch.size must_== 3
      messageBatch.sizeInBytes must_== 100 + 150 + 200 + MessageSet.LogOverhead * 3
      messageBatch.nextOffset must_== 53
      messageBatch.maxOffset must_== 52

      val mb2 = messageBatch.createBatchFromOffset(50).get
      // When creating an offset that is the same offset as the batch
      // we return back the same object
      messageBatch eq mb2 must_== true

      mb2.offset must_== 50
      mb2.size must_== 3
      mb2.sizeInBytes must_== 100 + 150 + 200 + MessageSet.LogOverhead * 3
      mb2.nextOffset must_== 53
      mb2.maxOffset must_== 52

      val mb3 = messageBatch.createBatchFromOffset(51).get
      mb3.offset must_== 51
      mb3.size must_== 2
      mb3.sizeInBytes must_== 150 + 200 + MessageSet.LogOverhead * 2
      mb3.nextOffset must_== 53
      mb3.maxOffset must_== 52

      val mb4 = messageBatch.createBatchFromOffset(52).get
      mb4.offset must_== 52
      mb4.size must_== 1
      mb4.sizeInBytes must_== 200 + MessageSet.LogOverhead
      mb4.nextOffset must_== 53
      mb4.maxOffset must_== 52
    }

    "return None if a message is not found" in {
      val a = makeMockMessageAndOffset(50, 51, 100)
      val b = makeMockMessageAndOffset(51, 52, 150)
      val c = makeMockMessageAndOffset(52, 53, 200)

      val mockMessageSet = makeMockMessageSet(List(a, b, c))
      val messageBatch = CastleMessageBatch(mockMessageSet)

      messageBatch.createBatchFromOffset(49) must_== None
      messageBatch.createBatchFromOffset(50).get.offset must_== 50
      messageBatch.createBatchFromOffset(50).get.size must_== 3
      messageBatch.createBatchFromOffset(50).get.sizeInBytes must_== 100 + 150 + 200 + MessageSet.LogOverhead * 3
      messageBatch.createBatchFromOffset(50).get.nextOffset must_== 53
      messageBatch.createBatchFromOffset(50).get.maxOffset must_== 52

      messageBatch.createBatchFromOffset(51).get.offset must_== 51
      messageBatch.createBatchFromOffset(51).get.size must_== 2
      messageBatch.createBatchFromOffset(51).get.sizeInBytes must_== 150 + 200 + MessageSet.LogOverhead * 2
      messageBatch.createBatchFromOffset(51).get.nextOffset must_== 53
      messageBatch.createBatchFromOffset(51).get.maxOffset must_== 52

      messageBatch.createBatchFromOffset(52).get.offset must_== 52
      messageBatch.createBatchFromOffset(52).get.size must_== 1
      messageBatch.createBatchFromOffset(52).get.sizeInBytes must_== 200 + MessageSet.LogOverhead
      messageBatch.createBatchFromOffset(52).get.nextOffset must_== 53
      messageBatch.createBatchFromOffset(52).get.maxOffset must_== 52

      messageBatch.createBatchFromOffset(53) must_== None

    }

    "allow a legitimate 0 size message (plus overhead), and correctly slice 0 size messages" in {
      val a = makeMockMessageAndOffset(30, 31, MessageOverhead)
      val b = makeMockMessageAndOffset(31, 32, MessageOverhead)
      val c = makeMockMessageAndOffset(32, 33, MessageOverhead)
      val d = makeMockMessageAndOffset(33, 34, MessageOverhead)
      val e = makeMockMessageAndOffset(34, 35, MessageOverhead)

      val mockMessageSet = makeMockMessageSet(List(a, b, c, d, e))
      val messageBatch = CastleMessageBatch(mockMessageSet)
      messageBatch.offset must_== 30
      messageBatch.size must_== 5
      messageBatch.sizeInBytes must_== 0 + (MessageSet.LogOverhead + MessageOverhead) * 5
      messageBatch.nextOffset must_== 35
      messageBatch.maxOffset must_== 34

      messageBatch.createBatchFromOffset(31).get.offset must_== 31
      messageBatch.createBatchFromOffset(31).get.size must_== 4
      messageBatch.createBatchFromOffset(31).get.sizeInBytes must_== 0 + (MessageSet.LogOverhead + MessageOverhead) * 4
      messageBatch.createBatchFromOffset(31).get.nextOffset must_== 35
      messageBatch.createBatchFromOffset(31).get.maxOffset must_== 34

      messageBatch.createBatchFromOffset(32).get.offset must_== 32
      messageBatch.createBatchFromOffset(32).get.size must_== 3
      messageBatch.createBatchFromOffset(32).get.sizeInBytes must_== 0 + (MessageSet.LogOverhead + MessageOverhead) * 3
      messageBatch.createBatchFromOffset(32).get.nextOffset must_== 35
      messageBatch.createBatchFromOffset(32).get.maxOffset must_== 34
    }
  }

}
