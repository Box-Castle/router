package com.box.castle.router

import akka.actor.Actor
import com.box.castle.router.messages.{FetchData, _}
import org.slf4s.Logging

import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration.FiniteDuration

/**
 * This trait assumes that for any of the request types a client can make to the Router, the client will only have one
 * outstanding request of that type at a time.  For example, both FetchData and FetchOffset requests can be
 * outstanding at the same time, but never 2 different FetchOffset requests for different offsets.
 *
 * If this assumption is not true, DO NOT mix in this trait.
 *
 * This trait is so clumsy because Akka is keeping its Receive intercept functionality INTERNAL, specifically:
 *
 *   protected[akka] def aroundReceive(receive: Actor.Receive, msg: Any): Unit = receive.applyOrElse(msg, unhandled)
 *
 * Otherwise, this trait could be changed to fit in more seamlessly with the given actor and completely hide
 * all this retry business as a self contained item in this trait.
 *
 * There is an attempt to expose this functionality here:
 * https://github.com/akka/akka/blob/master/akka-contrib/src/main/scala/akka/contrib/pattern/ReceivePipeline.scala
 *
 * It talks about how to use it here:
 * http://doc.akka.io/docs/akka/snapshot/contrib/receive-pipeline.html
 *
 * However, the "akka-contrib" subproject is for external contributions that *MIGHT* make it into the official release
 * at some point, and there are a bunch of caveat emptor warnings about binary compatibility and things changing left
 * and right, so we cannot use anything from there until it has made its way into the official branch.
 *
 * This ReceivePipeline just exposes that aroundReceive method since it's in the same akka namespace.
 *
 */
trait RouterRequestManager {
  selfType: Actor with Logging =>

  def router: RouterRef

  def fetchDataTimeout: FiniteDuration = Router.DefaultTimeout
  def fetchOffsetTimeout: FiniteDuration = Router.DefaultTimeout
  def fetchConsumerOffsetTimeout: FiniteDuration = Router.DefaultTimeout
  def commitConsumerOffsetTimeout: FiniteDuration = Router.DefaultTimeout
  def fetchTopicMetadataTimeout: FiniteDuration = Router.DefaultTimeout

  private abstract class RequestManager[T <: RouteToKafkaDispatcher, R <: RouterResult] {
    protected case class OutstandingRequestTracker(outstandingRequest: T, requestId: Long)

    protected var outstandingRequestTrackerOption: Option[OutstandingRequestTracker] = None
    var requestId: Long = 0

    def getTimeout: FiniteDuration
    def responseMatchesOutstandingRequest(response: R, outstandingRequest: T): Boolean
    def createRequestTimedOutMessage(request: T, requestId: Long): TimedOutRequest[T]

    def send(request: T): Unit = {
      requestId += 1
      outstandingRequestTrackerOption = Some(OutstandingRequestTracker(request, requestId))
      context.system.scheduler.scheduleOnce(getTimeout, self, createRequestTimedOutMessage(request, requestId))
      router ! request
    }

    /* The reason we check if our timed out request matches our outstanding request is because there are multiple
       ways where it might not match due to some race conditions.  Consider this set of events:

       1. Issue request K1 for offset 300, our outstanding request is for offset 300
       2. Setup timer A1 to self for 60 seconds
       3. Timer A1 times out, so we issue request K2 for offset 300, and setup timer A2 for 60 seconds, outstanding
          request is for offset 300 still (we do not modify it when retrying)
       4. Request K1 comes back for offset 300 a little after we timed out, it matches our outstanding request for 300
          We consider it to be a success so we set our outstanding request to None
       5. The application processes response for K1, and now requests the next offset 400
       6. Issue request L1 for offset 400, our outstanding request is for offset 400
       7. Setup timer A3 to self for 60 seconds, we must now treat this case as two simultaneously
          outstanding timers because we cannot cancel timer A2.  Cancelling A2 is not possible because A2 might
          have already sent its expiration message back to us before we have a chance to cancel it.  Alternatively,
          our request to cancel could be executed at an arbitrary point in the future and it could potentially expire
          before that point in the future anyway.  There is no reliably way to cancel it.
       8. Timer A2 times out, the offset it was requesting was 300, at this point we are requesting 400, so we
          can ignore this timeout as stale.

       There are a few other possibilities.  Such as not having a request for offset for 400, but having the outstanding
       request be None because we already successfully received it and we are now processing it.

       There are similar race conditions for when we get legitimate responses as well.
    */
    def handleTimeout(timedOutRequest: TimedOutRequest[T])(): Unit = {
      outstandingRequestTrackerOption.foreach(outstandingRequestTracker =>
        // For the DataFetch case, if the timedOutRequest.request matches the outstandingRequestTracker.outstandingRequest
        // but the requestId's do not match, then we are in a case where no data is being produced to the topic
        // and we keep getting 0 messages for it.  When we get 0 messages, we back off, and request the same offset again.
        // By checking the requestId, we can make sure we don't treat this case as a time out.
        if (timedOutRequest.request == outstandingRequestTracker.outstandingRequest &&
              timedOutRequest.requestId == outstandingRequestTracker.requestId) {
          log.warn(s"Request: ${timedOutRequest.request} (${timedOutRequest.requestId}) timed out, issuing it again")
          send(timedOutRequest.request)
        }
      )
    }

    // Handle response does not check the request id, so it can match responses that we get
    // back no matter how many times we requested them
    def handleResponse(response: R, responseProcessor: (R) => Unit): Unit = {
      outstandingRequestTrackerOption.foreach(outstandingRequestTracker => {
        if (responseMatchesOutstandingRequest(response, outstandingRequestTracker.outstandingRequest)) {
          outstandingRequestTrackerOption = None
          responseProcessor(response)
        }
      })
    }
  }

  private val fetchDataRequestManager = new RequestManager[FetchData, FetchData.Response] {
    override def getTimeout = fetchDataTimeout

    override def createRequestTimedOutMessage(request: FetchData, requestId: Long): TimedOutRequest[FetchData] =
      FetchData.TimedOut(request, requestId)

    override def responseMatchesOutstandingRequest(response: FetchData.Response,
                                                   outstandingRequest: FetchData): Boolean =
      response.offset == outstandingRequest.offset &&
        response.topicAndPartition == outstandingRequest.topicAndPartition
  }

  private val fetchOffsetRequestManager = new RequestManager[FetchOffset, FetchOffset.Response] {
    override def getTimeout: FiniteDuration = fetchOffsetTimeout

    override def createRequestTimedOutMessage(request: FetchOffset, requestId: Long): TimedOutRequest[FetchOffset] =
      FetchOffset.TimedOut(request, requestId)

    override def responseMatchesOutstandingRequest(response: FetchOffset.Response,
                                                   outstandingRequest: FetchOffset): Boolean =
      response.offsetType == outstandingRequest.offsetType
  }

  private val fetchConsumerOffsetRequestManager = new RequestManager[FetchConsumerOffset, FetchConsumerOffset.Response] {
    override def getTimeout: FiniteDuration = fetchConsumerOffsetTimeout

    override def createRequestTimedOutMessage(request: FetchConsumerOffset, requestId: Long): TimedOutRequest[FetchConsumerOffset] =
      FetchConsumerOffset.TimedOut(request, requestId)

    override def responseMatchesOutstandingRequest(response: FetchConsumerOffset.Response,
                                                   outstandingRequest: FetchConsumerOffset): Boolean =
      response.consumerId == outstandingRequest.consumerId
  }

  private val commitConsumerOffsetRequestManager = new RequestManager[CommitConsumerOffset, CommitConsumerOffset.Response] {
    override def getTimeout: FiniteDuration = commitConsumerOffsetTimeout

    override def createRequestTimedOutMessage(request: CommitConsumerOffset, requestId: Long): TimedOutRequest[CommitConsumerOffset] =
      CommitConsumerOffset.TimedOut(request, requestId)

    override def responseMatchesOutstandingRequest(response: CommitConsumerOffset.Response,
                                                   outstandingRequest: CommitConsumerOffset): Boolean =
      response.consumerId == outstandingRequest.consumerId &&
        response.offsetAndMetadata.offset == outstandingRequest.offsetAndMetadata.offset
  }

  private val fetchTopicMetadataRequestManager = new RequestManager[FetchTopicMetadata, FetchTopicMetadata.Response] {
    override def getTimeout: FiniteDuration = commitConsumerOffsetTimeout

    override def createRequestTimedOutMessage(request: FetchTopicMetadata, requestId: Long): TimedOutRequest[FetchTopicMetadata] =
      FetchTopicMetadata.TimedOut(request, requestId)

    override def responseMatchesOutstandingRequest(response: FetchTopicMetadata.Response,
                                                   outstandingRequest: FetchTopicMetadata): Boolean =
      response.requestId == outstandingRequest.requestId
  }

  def sendRequestToRouter(routable: RouteToKafkaDispatcher): Unit = {
    routable match {
      case request: FetchData            => fetchDataRequestManager.send(request)
      case request: FetchOffset          => fetchOffsetRequestManager.send(request)
      case request: FetchConsumerOffset  => fetchConsumerOffsetRequestManager.send(request)
      case request: CommitConsumerOffset => commitConsumerOffsetRequestManager.send(request)
      case request: FetchTopicMetadata   => fetchTopicMetadataRequestManager.send(request)
    }
  }

  def checkResult(result: FetchData.Result)(responseProcessor: (FetchData.Response) => Unit): Unit = {
    result match {
      case timedOut: FetchData.TimedOut => fetchDataRequestManager.handleTimeout(timedOut)
      case response: FetchData.Response => fetchDataRequestManager.handleResponse(response, responseProcessor)
    }
  }

  def checkResult(result: FetchOffset.Result)(responseProcessor: (FetchOffset.Response) => Unit): Unit = {
    result match {
      case timeOut: FetchOffset.TimedOut  => fetchOffsetRequestManager.handleTimeout(timeOut)
      case response: FetchOffset.Response => fetchOffsetRequestManager.handleResponse(response, responseProcessor)
    }
  }

  def checkResult(result: FetchConsumerOffset.Result)(responseProcessor: (FetchConsumerOffset.Response) => Unit): Unit = {
    result match {
      case timeOut: FetchConsumerOffset.TimedOut  => fetchConsumerOffsetRequestManager.handleTimeout(timeOut)
      case response: FetchConsumerOffset.Response => fetchConsumerOffsetRequestManager.handleResponse(response, responseProcessor)
    }
  }

  def checkResult(result: CommitConsumerOffset.Result)(responseProcessor: (CommitConsumerOffset.Response) => Unit): Unit = {
    result match {
      case timeOut: CommitConsumerOffset.TimedOut  => commitConsumerOffsetRequestManager.handleTimeout(timeOut)
      case response: CommitConsumerOffset.Response => commitConsumerOffsetRequestManager.handleResponse(response, responseProcessor)
    }
  }

  def checkResult(result: FetchTopicMetadata.Result)(responseProcessor: (FetchTopicMetadata.Response) => Unit): Unit = {
    result match {
      case timeOut: FetchTopicMetadata.TimedOut  => fetchTopicMetadataRequestManager.handleTimeout(timeOut)
      case response: FetchTopicMetadata.Response => fetchTopicMetadataRequestManager.handleResponse(response, responseProcessor)
    }
  }
}
