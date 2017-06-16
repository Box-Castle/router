package com.box.castle.router.kafkadispatcher

import java.util.concurrent.Executors

import akka.actor.Actor
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.RouterConfig
import com.box.castle.router.exceptions.RouterFatalException
import com.box.castle.router.kafkadispatcher.messages._
import com.box.castle.router.kafkadispatcher.processors._
import com.box.castle.router.messages.{BrokerUnreachable, RefreshBrokersAndLeaders}
import com.box.castle.router.metrics.{TagNames, Metrics, Components}
import com.box.kafka.Broker
import com.box.castle.consumer.CastleSimpleConsumerFactory
import org.slf4s.Logging

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

/**
 * This actor is responsible for dispatching requests to Kafka, processing them, and responding
 * to the original requester.  It is exclusively used by the Router.
 */
private[router]
class KafkaDispatcher(boxSimpleConsumerFactory: CastleSimpleConsumerFactory,
                      broker: Broker,
                      cacheSizeInBytes: Long,
                      routerConfig: RouterConfig,
                      metricsLogger: MetricsLogger)
  extends Actor with Logging {

  // We do this to avoid having to import scala.language.reflectiveCalls
  private class KafkaDispatcherExecutionContext extends ExecutionContext {
    val threadPool = Executors.newSingleThreadExecutor()

    def execute(runnable: Runnable): Unit = threadPool.submit(runnable)

    def reportFailure(t: Throwable): Unit = self ! UnexpectedFailure(new KafkaDispatcherException(
        "Unexpected failure in KafkaDispatcher thread ExecutionContext", t))

    def shutdown(): Unit = threadPool.shutdown()
  }

  private implicit val ec = new KafkaDispatcherExecutionContext()

  // The factory creates a local instance instance of BoxSimpleConsumer so that if the Actor is restarted
  // it will recreate a new copy of the consumer instead of using an old one that is passed in via Props.
  val boxSimpleConsumer = boxSimpleConsumerFactory.create(broker)

  val typedSelf = KafkaDispatcherRef(self)

  val commitConsumerOffsetProcessor = new CommitConsumerOffsetProcessor(typedSelf, boxSimpleConsumer, metricsLogger)
  val fetchConsumerOffsetProcessor = new FetchConsumerOffsetProcessor(typedSelf, boxSimpleConsumer, metricsLogger)
  val fetchOffsetProcessor = new FetchOffsetProcessor(typedSelf, boxSimpleConsumer, metricsLogger)
  val fetchTopicMetadataProcessor = new FetchTopicMetadataProcessor(typedSelf, context.system.scheduler, boxSimpleConsumer, metricsLogger)
  val fetchDataProcessor = new FetchDataProcessor(typedSelf, boxSimpleConsumer, cacheSizeInBytes, routerConfig, metricsLogger)

  val queueProcessors = new QueueProcessors(fetchConsumerOffsetProcessor,
                                            commitConsumerOffsetProcessor,
                                            fetchOffsetProcessor,
                                            fetchTopicMetadataProcessor,
                                            fetchDataProcessor)

  private def processKafkaResponse[T <: DispatchToKafka, T2 <: KafkaResponse]
  (response: T2, processor: QueueProcessor[T, T2]): Unit = {
    processor.processResponse(response)
    queueProcessors.getNextProcessorWithData(processor) match {
      case Some(nextProcessor) => nextProcessor.processQueue()
      case None => context.become(free)
    }
  }

  private def dispatchFree[T <: DispatchToKafka, T2 <: KafkaResponse]
  (request: T, processor: QueueProcessor[T, T2]): Unit = {
    processor.getFromCache(request) match {
      case Some(response) => request.requesterInfo.ref ! response
      case None => {
        context.become(busy)
        processor.addToQueue(request)
        processor.processQueue()
      }
    }
  }

  private def dispatchBusy[ReqType <: DispatchToKafka, ResType <: KafkaResponse]
  (request: ReqType, processor: QueueProcessor[ReqType, ResType]): Unit = {
    processor.getFromCache(request) match {
      case Some(response) => request.requesterInfo.ref ! response
      case None => processor.addToQueue(request)
    }
  }

  private def handleCommon(msg: KafkaDispatcherCommonMessage): Unit = {
    msg match {
      case ResizeCache(newCacheSizeInBytes) => fetchDataProcessor.setCacheSize(newCacheSizeInBytes)
      case LeaderNotAvailable(requests) =>
        context.parent ! RefreshBrokersAndLeaders(requests)
      case KafkaBrokerUnreachable(t) => {
        log.error(s"Broker is unreachable: $broker due to: ${t.getMessage}", t)
        context.parent ! BrokerUnreachable(broker)
      }
      case UnexpectedFailure(t) => {
        log.error(s"KafkaDispatcher encountered an unexpected failure: ${t.getMessage}", t)
        metricsLogger.count(Components.KafkaDispatcher,
          Metrics.UnexpectedFailures,
          Map(TagNames.Actor -> context.self.path.name))
        throw t
      }
    }
  }

  def busy: Receive = {
    case kafkaDispatcherMessage: KafkaDispatcherMessage => {
      kafkaDispatcherMessage match {
        case request: DispatchFetchConsumerOffsetToKafka => dispatchBusy(request, fetchConsumerOffsetProcessor)
        case response: FetchConsumerOffsetKafkaResponse => processKafkaResponse(response, fetchConsumerOffsetProcessor)

        case request: DispatchCommitConsumerOffsetToKafka => dispatchBusy(request, commitConsumerOffsetProcessor)
        case response: CommitConsumerOffsetKafkaResponse => processKafkaResponse(response, commitConsumerOffsetProcessor)

        case request: DispatchFetchOffsetToKafka => dispatchBusy(request, fetchOffsetProcessor)
        case response: FetchOffsetKafkaResponse => processKafkaResponse(response, fetchOffsetProcessor)

        case request: DispatchFetchTopicMetadataToKafka => dispatchBusy(request, fetchTopicMetadataProcessor)
        case response: InternalTopicMetadataResponse => processKafkaResponse(response, fetchTopicMetadataProcessor)

        case request: DispatchFetchDataToKafka => dispatchBusy(request, fetchDataProcessor)
        case response: FetchDataKafkaResponse => processKafkaResponse(response, fetchDataProcessor)

        case common: KafkaDispatcherCommonMessage => handleCommon(common)
      }
    }
  }

  def free: Receive = {
    case kafkaDispatcherMessage: KafkaDispatcherMessage => {
      kafkaDispatcherMessage match {
        case request: DispatchFetchConsumerOffsetToKafka  => dispatchFree(request, fetchConsumerOffsetProcessor)
        case request: DispatchCommitConsumerOffsetToKafka => dispatchFree(request, commitConsumerOffsetProcessor)
        case request: DispatchFetchOffsetToKafka          => dispatchFree(request, fetchOffsetProcessor)
        case request: DispatchFetchTopicMetadataToKafka   => dispatchFree(request, fetchTopicMetadataProcessor)
        case request: DispatchFetchDataToKafka            => dispatchFree(request, fetchDataProcessor)
        case response: KafkaResponse                      => throw new RouterFatalException(
          s"KafkaDispatcher received a KafkaResponse while in a free state: ${response.getClass.getCanonicalName}")
        case common: KafkaDispatcherCommonMessage         => handleCommon(common)
      }
    }
  }

  // Start off free
  def receive = free

  override def postStop(): Unit = {
    try {
      log.info(s"Shutting down KafkaDispatcher: $broker")
      boxSimpleConsumer.close()
      ec.shutdown()
    }
    catch {
      case NonFatal(e) => log.error(s"Ignoring exception thrown in KafkaDispatcher.postStop hook for $broker", e)
    }
  }
}