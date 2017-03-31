package com.box.castle.router.mock

import java.util.concurrent.atomic.AtomicLong

import com.box.castle.batch.CastleMessageBatch
import com.box.castle.collections.splitEvenly
import kafka.message._

trait MockBatchTools {

  val MessageOverhead = Message.CrcLength +
    Message.MagicLength +
    Message.AttributesLength +
    Message.KeySizeLength +
    Message.ValueSizeLength
  val MinimumMessageSize = MessageOverhead + 1
  val MinimumMessageSetSize = MinimumMessageSize + MessageSet.LogOverhead

  assert(MinimumMessageSetSize == 27)

  val messageSetLock = new Object()

  def makeMockMessageAndOffset(offset: Long, nextOffset: Long, byteSize: Int): MessageAndOffset = {
    require(byteSize >= MessageOverhead, s"byteSize $byteSize must be at least $MessageOverhead bytes")
    if (byteSize == MessageOverhead) {
      val message = new Message(
        null,
        null,
        NoCompressionCodec,
        0,
        0
      )
      MessageAndOffset(message, offset)
    }
    else {
      require(offset + 1 == nextOffset, s"offsets must be consecutive, got offset=$offset, nextOffset=$nextOffset")

      val payloadSize = byteSize - MessageOverhead
      require(payloadSize > 0)

      val arr = new Array[Byte](payloadSize)
      (0 until payloadSize).foreach(x => arr(x) = x.toByte)

      val message = new Message(
        arr,
        null,
        NoCompressionCodec,
        0,
        payloadSize
      )
      MessageAndOffset(message, offset)
    }
  }

  def makeMockMessageSet(mockMessageAndOffsets: Iterable[MessageAndOffset], requestedOffset: Long = 0): MessageSet = {
    val messageSetOffset = mockMessageAndOffsets.headOption match {
      case Some(x) => x.offset
      case None => requestedOffset
    }

    val messages: Seq[Message] = mockMessageAndOffsets.map(_.message).toSeq
    messageSetLock.synchronized {
      new ByteBufferMessageSet(NoCompressionCodec, new AtomicLong(messageSetOffset), messages: _*)
    }
  }

  def makeMockMessageSet(offset: Long, numMessages: Int, sizeInBytes: Int): MessageSet = {
    if (numMessages > 0) {
      val overheadForAllMessages = numMessages * MessageSet.LogOverhead
      val bytesAvailableForMessages = sizeInBytes - overheadForAllMessages
      val bytesPerMessage = splitEvenly(0 until bytesAvailableForMessages, numMessages)
      val messageList = (0 until numMessages).map(i  => {
        makeMockMessageAndOffset(offset + i, offset + i + 1, bytesPerMessage(i).size)
      }).toList

      makeMockMessageSet(messageList)
    }
    else {
      makeMockMessageSet(Seq.empty, offset)
    }
  }

  def createBatch(offset: Long, numMessages: Int, sizeInBytes: Int): CastleMessageBatch = {
    require(sizeInBytes >= MinimumMessageSize, s"createBatch sizeInBytes must be at least $MinimumMessageSize bytes")
    val messageSet = makeMockMessageSet(offset, numMessages, sizeInBytes)
    require(messageSet.sizeInBytes == sizeInBytes,
      s"requested sizeInBytes=$sizeInBytes, messageSet.sizeInBytes=${messageSet.sizeInBytes}")
    CastleMessageBatch(messageSet)
  }
}
