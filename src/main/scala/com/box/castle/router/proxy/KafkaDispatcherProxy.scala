package com.box.castle.router.proxy

import akka.actor.PoisonPill
import com.box.castle.router.kafkadispatcher._
import com.box.castle.router.kafkadispatcher.messages._

/**
 * This class abstracts away the fact that we are using two separate KafkaDispatcherActors within the Router,
 * one for fetching data from Kafka, and the other for all remaining requests.  This allows the message fetching actor
 * to not waste time on any other processing, it will continuously fetch messages.
 */
private[proxy]
class KafkaDispatcherProxy(kafkaDispatcher: KafkaDispatcherRef, fetchDataOnlyKafkaDispatcher: KafkaDispatcherRef) {

  def !(kafkaDispatcherMessage: KafkaDispatcherMessage): Unit = {
    kafkaDispatcherMessage match {
      case dispatchToKafka: DispatchToKafka => {
        dispatchToKafka match {
          case request: DispatchFetchDataToKafka => fetchDataOnlyKafkaDispatcher ! request
          case request @ (DispatchCommitConsumerOffsetToKafka(_,_,_,_) |
                          DispatchFetchConsumerOffsetToKafka(_,_,_) |
                          DispatchFetchOffsetToKafka(_,_,_) |
                          DispatchFetchTopicMetadataToKafka(_,_)) => kafkaDispatcher ! request
        }
      }
      case request: KafkaDispatcherCommonMessage => {
        fetchDataOnlyKafkaDispatcher ! request
        kafkaDispatcher ! request
      }
      // We should never receive KafkaResponses
      case _: KafkaResponse => throw new IllegalStateException()
    }
  }

  def kill(): Unit = {
    kafkaDispatcher ! PoisonPill
    fetchDataOnlyKafkaDispatcher ! PoisonPill
  }
}


