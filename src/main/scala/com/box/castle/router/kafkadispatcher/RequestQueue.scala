package com.box.castle.router.kafkadispatcher

import com.box.castle.collections.immutable.LinkedHashMap
import com.box.castle.router.kafkadispatcher.processors.RequesterInfo
import org.slf4s.Logging

/**
 * RequestQueue is a a convenient wrapper around a set of key -> queue pairs where each queue is represented by
 * a LinkedHashMap.  For the RequestQueue "removing the head" is equivalent to removing a single element from
 * each of the Subqueues associated with each key. Furthermore, RequestQueue guarantees that each Subqueue has
 * at least one RequesterInfo element associated with the Subqueue Key.
 * {{{
 * For example if our RequestQueue has this shape:
 *    "key1" -> LinkedHashMap(20 -> RI1, 25 -> RI2, 26 -> RI3)
 *    "key2" -> LinkedHashMap(870 -> RI54, 890 -> RI55)
 *    "key3" -> LinkedHashMap(70 -> RI80)
 *
 * Calling "removeHead" would return a Map:
 *    "key1" -> (20, RI1)
 *    "key2" -> (870, RI54)
 *    "key3" -> (70, RI80)
 *
 * And the new request quest would look like this:
 *    "key1" -> LinkedHashMap(25 -> RI2, 26 -> RI3)
 *    "key2" -> LinkedHashMap(890 -> RI55)
 *
 * }}}
 */
class RequestQueue[A, B] private (val subqueues: Map[A, LinkedHashMap[B, Set[RequesterInfo]]]) extends Logging {

  subqueues foreach {
    case (key, subqueue) => {
      require(!subqueue.isEmpty, s"subqueue for key $key must not be empty")
      subqueue.foreach {
        case (subqueueKey, requesters) =>
          require(requesters.nonEmpty,
            s"there must be at least one requester associated with key:$key, subqueueKey:$subqueueKey")
      }
    }
  }

  def add(key: A, subqueueKey: B, requesterInfo: RequesterInfo): RequestQueue[A, B] = {
    val existingSubqueue = subqueues(key)
    val existingRequesters = existingSubqueue(subqueueKey)

    val newRequesters = existingRequesters + requesterInfo
    val newSubqueue = existingSubqueue + (subqueueKey -> newRequesters)

    new RequestQueue(subqueues + (key -> newSubqueue))
  }

  /**
   * Removes subqueueKey from the subqueue associated with the the given key.
   * If this action will result in an empty subqueue then the entire key is removed from the RequestQueue.
   * @param key
   * @param subqueueKey
   * @return
   */
  def remove(key: A, subqueueKey: B): RequestQueue[A, B] = {
    subqueues.get(key) match {
      case Some(subqueue) => {
        val newSubqueue = subqueue - subqueueKey
        if (newSubqueue.isEmpty) {
          new RequestQueue(subqueues - key)
        }
        else {
          new RequestQueue(subqueues + (key -> newSubqueue))
        }
      }
      case None => this
    }
  }

  /**
   * Removes this key and all of its subqueues from the RequestQueue
   */
  def remove(key: A): RequestQueue[A, B] = new RequestQueue(subqueues - key)

  def removeHead(): (Map[A, (B, Set[RequesterInfo])], RequestQueue[A, B]) = {
    var newSubqueues = subqueues.empty
    val requests = subqueues map {
      case (topicAndPartition, subqueue) => {
        assert(!subqueue.isEmpty, "subqueue must not be empty")

        val (subqueueKey, requesterActorRefs, newSubqueue) = subqueue.removeHead()

        if (!newSubqueue.isEmpty)
          newSubqueues = newSubqueues + (topicAndPartition -> newSubqueue)

        (topicAndPartition, (subqueueKey, requesterActorRefs))
      }
    }

    (requests, new RequestQueue(newSubqueues))
  }

  def get(key: A): Option[LinkedHashMap[B, Set[RequesterInfo]]] = subqueues.get(key)

  def isEmpty: Boolean = subqueues.isEmpty

  override def toString: String = subqueues.toString()
}

object RequestQueue {
  private def emptySubqueue[A, B]: Map[A, LinkedHashMap[B, Set[RequesterInfo]]] =
    Map.empty[A, LinkedHashMap[B, Set[RequesterInfo]]].withDefaultValue(
      LinkedHashMap.empty[B, Set[RequesterInfo]].withDefaultValue(Set.empty[RequesterInfo]))

  def empty[A, B]: RequestQueue[A, B] = new RequestQueue(emptySubqueue[A, B])
}