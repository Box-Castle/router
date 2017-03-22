package com.box.castle.collections.immutable

import java.util.NoSuchElementException

import scala.collection.immutable.{Map, Queue}

/**
  * An immutable implementation of a LinkedHashMap.  Iterating over keys/values is done in insertion order.
  * Updating the values associated with keys that already exist in the map does not affect their insertion order.
  *
  * Performance characteristics:
  *  Adding new keys and updating existing keys is O(1)
  *  Removing the oldest inserted key is amortized O(1) when called successively. Prefer removeHead method when removing
  *  the oldest key.
  *  Removing an arbitrary key is O(n) with respect to the number of keys in the map
  */
class LinkedHashMap[A, +B] protected (private val queue: Queue[A],
                                      private val backingMap: Map[A, B])
    extends Map[A, B] {

  /**
    * Returns the keys in insertion order
    * @return
    */
  override def keys: Iterable[A] = queue

  /**
    * Returns the values in insertion order
    * @return
    */
  override def values: Iterable[B] = keys.map(k => backingMap(k))

  /**
    * Returns a new LinkedHashMap with key updated with the given value.  If the key already exists in the
    * LinkedHashMap, it's insertion position is not changed.
    * @tparam B1     type of the value of the new binding which is a supertype of `B`
    * @param key     the key that should be updated
    * @param value   the value to be associated with `key`
    * @return
    */
  override def updated[B1 >: B](key: A, value: B1): LinkedHashMap[A, B1] =
    backingMap.get(key) match {
      case Some(_) => new LinkedHashMap(queue, backingMap + (key -> value))
      case None =>
        new LinkedHashMap(queue.enqueue(key), backingMap + (key -> value))
    }

  /**
    * Removes the first inserted key from this Map.  This is an amortized O(1) operation.
    * @return the first inserted key, its corresponding value, and a new LinkedHashMap with the first key removed
    * @throws NoSuchElementException if this is called on an empty map
    */
  def removeHead(): (A, B, LinkedHashMap[A, B]) =
    if (queue.isEmpty) {
      throw new NoSuchElementException("removeHead on empty LinkedHashMap")
    } else {
      val (key, newQueue) = queue.dequeue
      (key, backingMap(key), new LinkedHashMap(newQueue, backingMap - key))
    }

  /**
    * @return the head element, will throw an exception if called on an empty LinkedHashMap.  See headOption for
    * the exception free version of this method.
    */
  override def head: (A, B) =
    if (queue.isEmpty)
      throw new NoSuchElementException("head on empty LinkedHashMap")
    else
      (queue.head, backingMap(queue.head))

  /**
    * @return an option of the head element, or None if the LinkedHashMap is empty.
    */
  override def headOption: Option[(A, B)] =
    for {
      k <- queue.headOption
      v <- backingMap.get(k)
    } yield (k, v)

  /**
    * @return the last element, will throw an exception if called on an empty LinkedHashMap.  See lastOption for
    * the exception free version of this method.
    */
  override def last: (A, B) =
    if (queue.isEmpty)
      throw new NoSuchElementException("last on empty LinkedHashMap")
    else
      (queue.last, backingMap(queue.last))

  /**
    * @return an option of the last element, or None if the LinkedHashMap is empty.
    */
  override def lastOption: Option[(A, B)] =
    for {
      k <- queue.lastOption
      v <- backingMap.get(k)
    } yield (k, v)

  /**
    * @return true if the LinkedHashMap has no element
    */
  override def isEmpty: Boolean = queue.isEmpty

  /**
    * Convenience method for updated
    */
  override def +[B1 >: B](kv: (A, B1)): LinkedHashMap[A, B1] =
    updated(kv._1, kv._2)

  /**
    * @param key the key used to look up the associated value within the LinkedHashMap
    * @return an option of the value associated with the given key, or None
    */
  override def get(key: A): Option[B] = backingMap.get(key)

  /**
    * Convenience method to return a new empty LinkedHashMap
    * @return
    */
  override def empty: LinkedHashMap[A, B] =
    new LinkedHashMap(Queue.empty[A], backingMap.empty)

  /**
    * @return an iterator over this collection
    */
  override def iterator: Iterator[(A, B)] = {
    val queueIterator = queue.iterator
    Iterator.fill(queue.size)({
      val key = queueIterator.next()
      val value = backingMap(key)
      (key, value)
    })
  }

  /** Retrieves the value which is associated with the given key. This
    *  method invokes the `default` method of the map if there is no mapping
    *  from the given key to a value. Unless overridden, the `default` method throws a
    *  `NoSuchElementException`.
    *
    *  @param  key the key
    *  @return     the value associated with the given key, or the result of the
    *              map's `default` method, if none exists.
    */
  override def apply(key: A): B = backingMap.apply(key)

  /**
    * When removing the first inserted key, this is an amortized O(1) operation.
    * When removing any other key, this is an O(n) operation
    * Removing a non existent key returns the map unchanged.
    * @param key the key to remove
    * @return
    */
  override def -(key: A): LinkedHashMap[A, B] =
    if (backingMap.contains(key))
      if (queue.head == key)
        new LinkedHashMap(queue.dequeue._2, backingMap - key)
      else
        new LinkedHashMap(queue.filter(o => o != key), backingMap - key)
    else
      this

  /**
    * @return number of elements in the collection
    */
  override def size: Int = backingMap.size

  /** The same map with a given default function.
    *  Note: `get`, `contains`, `iterator`, `keys`, etc are not affected by `withDefault`.
    *
    *  Invoking transformer methods (e.g. `map`) will not preserve the default value.
    *
    *  @param d     the function mapping keys to values, used for non-present keys
    *  @return      a wrapper of the map with a default value
    */
  override def withDefault[B1 >: B](d: A => B1): LinkedHashMap[A, B1] =
    new LinkedHashMap(queue, backingMap.withDefault(d))

  /** The same map with a given default value.
    *  Note: `get`, `contains`, `iterator`, `keys`, etc are not affected by `withDefaultValue`.
    *
    *  Invoking transformer methods (e.g. `map`) will not preserve the default value.
    *
    *  @param d     the function mapping keys to values, used for non-present keys
    *  @return      a wrapper of the map with a default value
    */
  override def withDefaultValue[B1 >: B](d: B1): LinkedHashMap[A, B1] =
    new LinkedHashMap(queue, backingMap.withDefaultValue(d))

  override def canEqual(other: Any): Boolean =
    other.isInstanceOf[LinkedHashMap[A, B]]

  override def equals(other: Any): Boolean =
    other match {
      case that: LinkedHashMap[A, B] =>
        (that canEqual this) &&
          queue == that.queue &&
          backingMap == that.backingMap
      case _ => false
    }

  override def hashCode(): Int = {
    val state = Seq(queue, backingMap)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString(): String =
    queue
      .map(k => s"$k -> ${backingMap(k)}")
      .mkString("LinkedHashMap(", ", ", ")")
}

object LinkedHashMap {
  def empty[A, B]: LinkedHashMap[A, B] =
    new LinkedHashMap[A, B](Queue.empty[A], Map.empty[A, B])

  def apply[A, B](args: (A, B)*): LinkedHashMap[A, B] =
    args.foldLeft(empty[A, B])((hashMap, kvp) => hashMap + (kvp._1 -> kvp._2))
}
