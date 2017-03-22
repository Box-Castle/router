package com.box.castle.router.kafkadispatcher

import com.box.castle.collections.immutable.LinkedHashMap
import com.box.castle.router.kafkadispatcher.processors.RequesterInfo
import kafka.common.TopicAndPartition
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

/**
 * Created by dgrenader on 3/28/15.
 */
class RequestQueueTest extends Specification with Mockito {

  "RequestQueue" should {
    "properly initialize the default value of the underlying map to be an empty offset queue" in {
      val actorRef2 = mock[RequesterInfo]
      val actorRef3 = mock[RequesterInfo]

      val tp = TopicAndPartition("abc", 123)
      var requestQueue = RequestQueue.empty.add(tp, 2L, actorRef2).add(tp, 3L, actorRef3)

      // This should succeed without issues
      val tp2 = TopicAndPartition("xyz", 789)
      requestQueue = requestQueue.add(tp2, 4L, actorRef3)

      val (requests, _) = requestQueue.removeHead()
      requests must_== Map(tp -> ((2, Set(actorRef2))), tp2 -> ((4, Set(actorRef3))))
    }

    "handle the empty case" in {
      RequestQueue.empty.isEmpty must_== true
    }

    "handle the basic case" in {
      RequestQueue.empty.isEmpty must_== true

      val actorRef1 = mock[RequesterInfo]
      val topicAndPartition = TopicAndPartition("perf", 1)
      val rq = RequestQueue.empty.add(topicAndPartition, 300L, actorRef1)
      rq.isEmpty must_== false

      val (requests, newRequestQueue) = rq.removeHead()

      requests must_== Map(topicAndPartition -> ((300L, Set(actorRef1))))
      newRequestQueue.isEmpty must_== true
    }

    "continue to have correct default values when removing head" in {
      var rq = RequestQueue.empty[TopicAndPartition, Long]
      val requesterInfo = mock[RequesterInfo]
      val topicAndPartition = TopicAndPartition("perf", 1)
      rq = rq.add(topicAndPartition, 2500L, requesterInfo)

      val (requests, newRequestQueue) = rq.removeHead()
      requests must_== Map(topicAndPartition -> ((2500L, Set(requesterInfo))))
      newRequestQueue.isEmpty must_== true

      rq = newRequestQueue

      val requesterInfo2 = mock[RequesterInfo]
      val topicAndPartition2 = TopicAndPartition("perf", 2)
      rq = rq.add(topicAndPartition2, 3600L, requesterInfo2)

      val (requests2, newRequestQueue2) = rq.removeHead()
      requests2 must_== Map(topicAndPartition2 -> ((3600L, Set(requesterInfo2))))
      newRequestQueue2.isEmpty must_== true
    }

    "handle multiple requests for the same topic and partition" in {
      val actorRef1 = mock[RequesterInfo]
      val actorRef2 = mock[RequesterInfo]
      val actorRef3 = mock[RequesterInfo]
      val actorRef4 = mock[RequesterInfo]

      val topicAndPartition = TopicAndPartition("perf", 1)
      val rq = RequestQueue.empty
        .add(topicAndPartition, 300L, actorRef1)
        .add(topicAndPartition, 400L, actorRef2)
        .add(topicAndPartition, 500L, actorRef3)
        .add(topicAndPartition, 300L, actorRef4)
      rq.isEmpty must_== false

      val (requests, newRequestQueue) = rq.removeHead()
      requests must_== Map(topicAndPartition -> ((300L, Set(actorRef1, actorRef4))))
      newRequestQueue.isEmpty must_== false

      val (requests2, newRequestQueue2) = newRequestQueue.removeHead()
      requests2 must_== Map(topicAndPartition -> ((400L, Set(actorRef2))))
      newRequestQueue2.isEmpty must_== false

      val (requests3, newRequestQueue3) = newRequestQueue2.removeHead()
      requests3 must_== Map(topicAndPartition -> ((500L, Set(actorRef3))))
      newRequestQueue3.isEmpty must_== true
    }

    "handle multiple requests for different topic and partitions" in {
      val actorRef1 = mock[RequesterInfo]
      val actorRef2 = mock[RequesterInfo]
      val actorRef3 = mock[RequesterInfo]
      val actorRef4 = mock[RequesterInfo]

      val topicAndPartition = TopicAndPartition("perf", 1)
      val topicAndPartition2 = TopicAndPartition("job_manager", 2)

      val rq = RequestQueue.empty
        .add(topicAndPartition, 300L, actorRef1)
        .add(topicAndPartition2, 400L, actorRef2)
        .add(topicAndPartition, 500L, actorRef3)
        .add(topicAndPartition, 300L, actorRef4)
      rq.isEmpty must_== false

      rq.get(topicAndPartition) must_== Some(LinkedHashMap(300L -> Set(actorRef1, actorRef4), 500L -> Set(actorRef3)))

      val (requests, newRequestQueue) = rq.removeHead()
      requests must_== Map(topicAndPartition -> ((300L, Set(actorRef1, actorRef4))),
        topicAndPartition2 -> ((400L, Set(actorRef2))))
      newRequestQueue.isEmpty must_== false

      val (requests2, newRequestQueue2) = newRequestQueue.removeHead()
      requests2 must_== Map(topicAndPartition -> ((500L, Set(actorRef3))))
      newRequestQueue2.isEmpty must_== true
    }

    "properly remove from an empty queue" in {
      RequestQueue.empty.remove(TopicAndPartition("x", 1), 500).subqueues must_== RequestQueue.empty.subqueues
      RequestQueue.empty.remove(TopicAndPartition("x", 1)).subqueues must_== RequestQueue.empty.subqueues
    }

    "handle requests for the exact same offset multiple times from the same actor" in {
      val actorRef1 = mock[RequesterInfo]
      val actorRef2 = mock[RequesterInfo]
      val tp = TopicAndPartition("perf", 1)
      val rq = RequestQueue.empty
        .add(tp, 25, actorRef1)
        .add(tp, 25, actorRef2)
        .add(tp, 25, actorRef1)
        .add(tp, 25, actorRef2)
        .add(tp, 25, actorRef1)
        .add(tp, 25, actorRef2)

      val (requests, newRequestQueue) = rq.removeHead()
      requests must_== Map(tp -> ((25, Set(actorRef1, actorRef2))))

      newRequestQueue.isEmpty must_== true
    }

    "properly remove a single offset from a queue" in {
      val actorRef1 = mock[RequesterInfo]
      val actorRef2 = mock[RequesterInfo]
      val actorRef3 = mock[RequesterInfo]

      val tp = TopicAndPartition("abc", 123)
      val requestQueue = RequestQueue.empty.add(tp, 1L, actorRef1).add(tp, 2L, actorRef2).add(tp, 3L, actorRef3)
      requestQueue.remove(tp, 100000L).subqueues must_== requestQueue.subqueues

      requestQueue.remove(tp, 1L).subqueues must_== RequestQueue.empty.add(tp, 2L, actorRef2).add(tp, 3L, actorRef3).subqueues
      requestQueue.remove(tp, 2L).subqueues must_== RequestQueue.empty.add(tp, 1L, actorRef1).add(tp, 3L, actorRef3).subqueues
      requestQueue.remove(tp, 3L).subqueues must_== RequestQueue.empty.add(tp, 1L, actorRef1).add(tp, 2L, actorRef2).subqueues

      requestQueue.remove(tp, 1).remove(tp, 2).remove(tp, 3).subqueues must_== RequestQueue.empty.subqueues
      requestQueue.remove(tp, 1).remove(tp, 3).remove(tp, 2).subqueues must_== RequestQueue.empty.subqueues
      requestQueue.remove(tp, 2).remove(tp, 1).remove(tp, 3).subqueues must_== RequestQueue.empty.subqueues
      requestQueue.remove(tp, 2).remove(tp, 3).remove(tp, 1).subqueues must_== RequestQueue.empty.subqueues
      requestQueue.remove(tp, 3).remove(tp, 2).remove(tp, 1).subqueues must_== RequestQueue.empty.subqueues
      requestQueue.remove(tp, 3).remove(tp, 1).remove(tp, 2).subqueues must_== RequestQueue.empty.subqueues
    }
  }
}