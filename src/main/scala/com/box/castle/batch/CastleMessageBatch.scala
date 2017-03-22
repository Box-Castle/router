package com.box.castle.batch

import java.nio.ByteBuffer

import kafka.message.{ByteBufferMessageSet, Message, MessageAndOffset, MessageSet}

/**
 * CastleMessageBatch is guaranteed to have a least one message
 */
case class CastleMessageBatch(messageAndOffsetSeq: IndexedSeq[MessageAndOffset], sizeInBytes: Int) {

  require(messageAndOffsetSeq.nonEmpty, "CastleMessageBatch must not be empty")
  messageAndOffsetSeq.foldLeft(Long.MinValue)((prevOffset, messageAndOffset) => {
    require(prevOffset < messageAndOffset.offset,
      s"offsets must be monotonically increasing, prev offset: $prevOffset, current offset: ${messageAndOffset.offset}")
    require(messageAndOffset.offset < messageAndOffset.nextOffset,
      s"offset ${messageAndOffset.offset} must be less than next offset ${messageAndOffset.nextOffset} within a message")
    messageAndOffset.offset
  })

  val offset = messageAndOffsetSeq.head.offset

  val nextOffset: Long = messageAndOffsetSeq.last.nextOffset

  val messageSeq: IndexedSeq[Message] = messageAndOffsetSeq.map(_.message)

  /**
   * The number of messages in the batch, for the size of the batch in bytes use sizeInBytes.
   * This will be by definition one or more.  It will never be 0.
   */
  val size: Int = messageAndOffsetSeq.size

  /**
   * The maximum offset within this batch.  This is safe to call because CastleMessageBatch guarantees
   * there is at least one message in it.
   */
  val maxOffset: Long = messageAndOffsetSeq.last.offset

  private def indexOf(offset: Long): Int = {
    if (offset < this.offset || offset > messageAndOffsetSeq.last.offset) {
      -1
    }
    else {
      val mostLikelyIdx = (offset - this.offset).toInt
      if (mostLikelyIdx >= 0 && mostLikelyIdx < messageAndOffsetSeq.size && messageAndOffsetSeq(mostLikelyIdx).offset == offset) {
        // This is the common case
        mostLikelyIdx
      }
      else {
        // Linear search :(
        messageAndOffsetSeq.indexWhere(messageAndOffset => messageAndOffset.offset == offset)
      }
    }
  }

  /**
   * Create a new CastleMessageBatch from the provided offset within the current CastleMessageBatch
   * @param startOffset
   * @return
   */
  def createBatchFromOffset(startOffset: Long): Option[CastleMessageBatch] = {
    val startIndex = indexOf(startOffset)
    if (startIndex < 0) {
      None
    }
    else if (this.offset == startOffset) {
      Some(this)
    }
    else {
      val newMessageAndOffsetSeq = messageAndOffsetSeq.slice(startIndex, messageAndOffsetSeq.length)
      // Calculate the size in bytes of the new slice of the messages
      val sizeInBytes = MessageSet.messageSetSize(newMessageAndOffsetSeq.map(messageAndOffset => messageAndOffset.message))
      Some(new CastleMessageBatch(newMessageAndOffsetSeq, sizeInBytes))
    }
  }

  override def toString: String =
    s"CastleMessageBatch(offset=$offset,nextOffset=$nextOffset,size=$size,sizeInBytes=$sizeInBytes,maxOffset=$maxOffset)"
}

object CastleMessageBatch {

  def apply(messageSet: MessageSet): CastleMessageBatch = {
    // This is very unfortunate, but we have to make a deep copy of the messageSet because the underlying
    // byte buffer is shared between all MessageSets returned in a single request.  This buffer can be
    // a few gigabytes big depending on the number and size of the MessageSets retrieved, and if we cache any
    // of these MessageSets locally for a prolonged period of time, we will hold on to the ENTIRE buffer
    // for the entire duration even if we are only interested in a tiny portion of it which will lead to OOM exceptions
    val messageSetCopy = deepCopy(messageSet)
    new CastleMessageBatch(toIndexedSeq(messageSetCopy), messageSet.sizeInBytes)
  }

  private def deepCopy(messageSet: MessageSet): MessageSet = {
    val original = messageSet.asInstanceOf[ByteBufferMessageSet].buffer.asReadOnlyBuffer()

    val clone = ByteBuffer.allocate(original.limit())

    original.rewind()  // Make sure data is copied from the beginning
    clone.put(original)
    clone.flip()

    new ByteBufferMessageSet(clone)
  }

  /**
   * Converts a message set into an indexed seq, if there is a corrupt message somewhere in the set, this will
   * convert all the messages up to that corrupt message into an indexed seq.  This allows the router to identify
   * the exact offset of the corrupt message so that it can potentially skip it or log the exact corrupt offset
   * depending on what the client policy is of the client using the router.
   * @param messageSet
   * @return
   */
  private def toIndexedSeq(messageSet: MessageSet): IndexedSeq[MessageAndOffset] = {
    try {
      messageSet.toIndexedSeq
    }
    catch {
      case ex: kafka.message.InvalidMessageException => {
        var messageIndexedSeq = IndexedSeq.empty[MessageAndOffset]

        try {
          messageSet.foreach(messageAndOffset => {
            messageIndexedSeq = messageIndexedSeq :+ messageAndOffset
          })
        }
        catch {
          case _: kafka.message.InvalidMessageException => // Ignore
        }

        if (messageIndexedSeq.isEmpty)
          throw ex
        else
          messageIndexedSeq
      }
    }
  }
}
