package com.box.castle.router.kafkadispatcher

import com.box.castle.router.kafkadispatcher.messages.{KafkaResponse, DispatchToKafka}
import com.box.castle.router.kafkadispatcher.processors.QueueProcessor

import scala.annotation.tailrec

/**
 * QueueProcessors manages the list of queue processors through which the KafkaDispatcher round robbins.
 * @param queueProcessors
 */
private[kafkadispatcher]
class QueueProcessors(queueProcessors: QueueProcessor[_ <: DispatchToKafka, _ <: KafkaResponse]*) {

  type QP = QueueProcessor[_ <: DispatchToKafka, _ <: KafkaResponse]

  private val queueProcessorIdxMap = queueProcessors.zipWithIndex.toMap

  /**
   * Returns the next queue processor that has items in its queue to process that comes after the given queue processor.
   * This method goes around in a round robin fashion, so it can potentially return the same exact queue processor that
   * was passed in if all the other ones after it have empty queues and this queue processor has items in its own queue.
   * In other words, our checking for non empty queues always starts off with the processor after the given one
   * and circles all the way back to check the passed in queue processor for items in its queue.
   *
   * Returns None if there no processors that have data to process.
   *
   * @return
   */
  def getNextProcessorWithData(queueProcessor: QP): Option[QP] = {
    // We start checking from the next queueProcessor in line
    val nextProcessor = next(queueProcessor)
    checkProcessorQueues(nextProcessor, 0)
  }

  /**
   * Given a queue processor, return the next one in a round robbin order
   * @return
   */
  private def next(queueProcessor: QP): QP = {
    val curProcessorIdx = queueProcessorIdxMap(queueProcessor)
    val nextIdx = (curProcessorIdx + 1) % queueProcessors.size
    queueProcessors(nextIdx)
  }

  @tailrec
  private def checkProcessorQueues(queueProcessor: QP, numProcessorsChecked: Int): Option[QP] = {
    if (numProcessorsChecked == queueProcessors.size) {
      // All the processors have empty request queues
      None
    }
    else {
      if (queueProcessor.isEmpty) {
        val nextProcessor = next(queueProcessor)
        // Try the next processor (with wrap around)
        checkProcessorQueues(nextProcessor, numProcessorsChecked + 1)
      }
      else {
        Some(queueProcessor)
      }
    }
  }
}
