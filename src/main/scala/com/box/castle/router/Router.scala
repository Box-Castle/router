package com.box.castle.router

import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, OneForOneStrategy}
import com.box.castle.metrics.MetricsLogger
import com.box.castle.router.exceptions.RouterFatalException
import com.box.castle.router.kafkadispatcher.messages.{DispatchToKafka, DispatchToKafkaWithTopicAndPartition, DispatchFetchTopicMetadataToKafka}
import com.box.castle.router.metrics.{TagNames, Metrics, Components}
import com.box.castle.router.proxy.KafkaDispatcherProxyPoolFactory
import com.box.castle.router.messages._
import com.box.kafka.Broker
import org.slf4s.Logging

import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

/**
 * There will be one instance of the Router actor.  It will have 2 KafkaDispatchers per broker
 * in Kafka as its children.  One for fetching messages only, and one for doing everything else.
 */
class Router(dispatcherProxyPoolFactory: KafkaDispatcherProxyPoolFactory,
             knownBrokers: Set[Broker],
             metricsLogger: MetricsLogger) extends Actor with Logging {

  override val supervisorStrategy =
    OneForOneStrategy() {
      case _: RouterFatalException => Escalate
      case _: IllegalArgumentException => Escalate
      case NonFatal(_) => Restart
      case t => super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
    }

  val dispatcherPool = dispatcherProxyPoolFactory.create(context)

  private var outstandingFetchTopicMetadataRequest: Option[FetchTopicMetadata] = None
  private var fetchTopicMetadataRequestId: Long = 0

  // We do this to avoid having to import scala.language.reflectiveCalls
  private class SchedulingExecutionContext extends ExecutionContext {
    val threadPool = Executors.newCachedThreadPool()

    def execute(runnable: Runnable): Unit = threadPool.submit(runnable)

    def reportFailure(t: Throwable): Unit = self ! UnexpectedFailure(new RouterException(
        "Unexpected failure in the SchedulingExecutionContext execution context for RouterActor", t))

    def shutdown(): Unit = threadPool.shutdown()
  }

  private val schedulingEC = new SchedulingExecutionContext()

  private var requestQueue = Queue.empty[DispatchToKafka]

  def refreshBrokersAndLeaders(): Unit = {
    // We always establish the baseline in case we have removed some of these brokers and they came back online
    dispatcherPool.updateBrokers(knownBrokers)

    // We will now update the leader map by fetching topic metadata
    context.become(fetchingTopicMetadata)

    fetchTopicMetadataRequestId += 1
    val request = FetchTopicMetadata(fetchTopicMetadataRequestId)
    // We send this message to ourselves at some point in the future to make sure we retry getting
    // all topics and partitions if for some reason the dispatcher does not return them to us
    context.system.scheduler.scheduleOnce(Router.DefaultTimeout,
      self, FetchTopicMetadata.TimedOut(request, 0))(schedulingEC)
    outstandingFetchTopicMetadataRequest = Some(request)
    dispatcherPool.random ! request.asDispatchToKafkaMessage(self)
  }

  private def handleUnexpectedFailure(t: Throwable): Unit = {
    metricsLogger.count(Components.Router,
      Metrics.UnexpectedFailures,
      Map(TagNames.Actor -> context.self.path.name))
    log.error(s"Router encountered an unexpected failure: ${t.getMessage}", t)
    throw t
  }

  private def processRequestQueue(): Unit = {
    var failedToDispatch = Queue.empty[DispatchToKafka]

    requestQueue.foreach {
      case request: DispatchToKafkaWithTopicAndPartition => {
        dispatcherPool.get(request.topicAndPartition) match {
          case Some(dispatcher) => dispatcher ! request
          case None => failedToDispatch = failedToDispatch.enqueue(request)
        }
      }
      case request: DispatchFetchTopicMetadataToKafka => {
        if (dispatcherPool.size > 0)
          dispatcherPool.random ! request
        else
          failedToDispatch = failedToDispatch.enqueue(request)
      }
    }

    if (failedToDispatch.nonEmpty) {
      requestQueue = failedToDispatch
      // If we failed to dispatch any of the requests, we refresh brokers and update our leader map
      // in case something is out of date
      refreshBrokersAndLeaders()
    }
    else {
      context.become(free)
      requestQueue = Queue.empty[DispatchToKafka]
    }
  }

  private def handleRouteToKafkaDispatcherBusy(kafkaDispatcherRequest: RouteToKafkaDispatcher): Unit = {
    kafkaDispatcherRequest.asDispatchToKafkaMessage(context.sender()) match {
      case request: DispatchToKafkaWithTopicAndPartition => {
        dispatcherPool.get(request.topicAndPartition) match {
          case Some(dispatcher) => dispatcher ! request
          case None => requestQueue = requestQueue.enqueue(request)
        }
      }
      case request: DispatchFetchTopicMetadataToKafka => {
        if (dispatcherPool.size > 0)
          dispatcherPool.random ! request
        else
          requestQueue = requestQueue.enqueue(request)
      }
    }
  }

  /**
   * We are busy fetching topic metadata right now, we can't immediately do any other actions
   * so we will have to queue them all up
   * @return
   */
  def fetchingTopicMetadata: Receive = {
    // All Receive methods must be able to process all children of RouterMessage, which is the top level type
    case routerMessage: RouterMessage => {
      routerMessage match {
        case internalRouterMessage: InternalRouterMessage => {
          internalRouterMessage match {
            case UnexpectedFailure(t) => handleUnexpectedFailure(t)
            case BrokerUnreachable(broker) => {
              log.info(s"Broker is unreachable: $broker and we are already in the middle of fetching topic metadata")

              // We are getting this message while fetching topic metadata.  It is possible that it is the
              // the broker we are requesting the metadata from is itself unreachable
              // that means we will never get back a fetch topic metadata success
              // However, we will get a FetchTopicMetadataExpired message and restart the process
              // so all we have to do is just remove this broker from the dispatcher pool
              dispatcherPool.removeBroker(broker)
            }
            case RefreshBrokersAndLeaders(requests) => {
              // We don't have to do anything since we are already in the middle of doing
              // this refresh, so we just add the requests to our queue
              requestQueue = requestQueue ++ requests
            }
          }
        }
        case routeToKafkaDispatcher: RouteToKafkaDispatcher => handleRouteToKafkaDispatcherBusy(routeToKafkaDispatcher)
      }
    }
    case result: FetchTopicMetadata.Result => {
      result match {
        case timeOut: FetchTopicMetadata.TimedOut => {
          outstandingFetchTopicMetadataRequest.foreach(outstandingRequest =>
            if (timeOut.request == outstandingRequest) {
              log.warn(s"Request: ${timeOut.request} timed out, issuing it again")
              context.system.scheduler.scheduleOnce(Router.DefaultTimeout, self, timeOut)(schedulingEC)
              dispatcherPool.random ! timeOut.request.asDispatchToKafkaMessage(self)
            }
          )
        }
        case response: FetchTopicMetadata.Success => {
          outstandingFetchTopicMetadataRequest.foreach(outstandingRequest => {
            if (response.requestId == outstandingRequest.requestId) {
              outstandingFetchTopicMetadataRequest = None
              dispatcherPool.updateLeaderMap(response.topicMetadata)
              // Process request queue will put us back in a free state if everything is alright
              processRequestQueue()
            }
          })
        }
      }
    }
  }

  /**
   * Free to service all incoming requests
   * @return
   */
  def free: Receive = {
    // All Receive methods must be able to process all children of RouterMessage, which is the top level type
    case routerMessage: RouterMessage => {
      routerMessage match {
        case routeToKafkaDispatcher: RouteToKafkaDispatcher => {
          routeToKafkaDispatcher.asDispatchToKafkaMessage(context.sender()) match {
            case request: DispatchToKafkaWithTopicAndPartition => {
              dispatcherPool.get(request.topicAndPartition) match {
                case Some(dispatcher) => dispatcher ! request
                case None => {
                  requestQueue = requestQueue.enqueue(request)
                  refreshBrokersAndLeaders()
                }
              }
            }
            case request: DispatchFetchTopicMetadataToKafka => {
              if (dispatcherPool.size > 0) {
                dispatcherPool.random ! request
              }
              else {
                requestQueue = requestQueue.enqueue(request)
                refreshBrokersAndLeaders()
              }
            }
          }
        }
        case internalDispatcherMessage: InternalRouterMessage => {
          internalDispatcherMessage match {
            case UnexpectedFailure(t) => handleUnexpectedFailure(t)
            case BrokerUnreachable(broker) => {
              log.info(s"Broker is unreachable: $broker, refreshing brokers and leaders")
              dispatcherPool.removeBroker(broker)
              refreshBrokersAndLeaders()
            }
            case RefreshBrokersAndLeaders(requests) => {
              requestQueue = requestQueue ++ requests
              refreshBrokersAndLeaders()
            }
          }
        }
      }
    }
  }

  // Start off free
  def receive = free

  override def postStop(): Unit = {
    try {
      log.info("RouterActor stopped")
      log.info("Shutting down the execution context ThreadPool")
      schedulingEC.shutdown()
    }
    catch {
      case NonFatal(t) => log.warn("RouterActor ignored exception while shutting down", t)
    }
  }
}

object Router {
  // Default timeout for requests sent TO the Router by client code
  val DefaultTimeout = FiniteDuration(60, TimeUnit.SECONDS)
}