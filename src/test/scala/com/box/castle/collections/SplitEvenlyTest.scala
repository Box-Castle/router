package com.box.castle.collections

import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

class SplitEvenlyTest extends Specification with Mockito {

  val bucket = IndexedSeq

  "splitEvenly" should {
    "split correctly when not excluding 0 sized buckets" in {
      // 1 bucket
      splitEvenly(IndexedSeq(), 1) must_== IndexedSeq(bucket())
      splitEvenly(IndexedSeq("a"), 1) must_== IndexedSeq(bucket("a"))
      splitEvenly(IndexedSeq("a", "b"), 1) must_== IndexedSeq(bucket("a", "b"))
      splitEvenly(IndexedSeq("a", "b", "c"), 1) must_== IndexedSeq(bucket("a", "b", "c"))

      // 2 buckets
      splitEvenly(IndexedSeq(), 2) must_== IndexedSeq(
        bucket(), bucket())

      splitEvenly(IndexedSeq("a"), 2) must_== IndexedSeq(
        bucket("a"), bucket())

      splitEvenly(IndexedSeq("a", "b"), 2) must_== IndexedSeq(
        bucket("a"), bucket("b"))

      splitEvenly(IndexedSeq("a", "b", "c"), 2) must_== IndexedSeq(
        bucket("a", "b"), bucket("c"))

      splitEvenly(IndexedSeq("a", "b", "c", "d"), 2) must_== IndexedSeq(
        bucket("a", "b"), bucket("c", "d"))

      // 3 buckets
      splitEvenly(IndexedSeq(), 3) must_== IndexedSeq(
        bucket(), bucket(), bucket())

      splitEvenly(IndexedSeq("a"), 3) must_== IndexedSeq(
        bucket("a"), bucket(), bucket())

      splitEvenly(IndexedSeq("a", "b"), 3) must_== IndexedSeq(
        bucket("a"), bucket("b"), bucket())

      splitEvenly(IndexedSeq("a", "b", "c"), 3) must_== IndexedSeq(
        bucket("a"), bucket("b"), bucket("c"))

      splitEvenly(IndexedSeq("a", "b", "c", "d"), 3) must_== IndexedSeq(
        bucket("a", "b"), bucket("c"), bucket("d"))

      splitEvenly(IndexedSeq("a", "b", "c", "d", "e"), 3) must_== IndexedSeq(
        bucket("a", "b"), bucket("c", "d"), bucket("e"))

      splitEvenly(IndexedSeq("a", "b", "c", "d", "e", "f"), 3) must_== IndexedSeq(
        bucket("a", "b"), bucket("c", "d"), bucket("e", "f"))

      splitEvenly(IndexedSeq("a", "b", "c", "d", "e", "f", "g"), 3) must_== IndexedSeq(
        bucket("a", "b", "c"), bucket("d", "e"), bucket("f", "g"))

      // 4 buckets
      splitEvenly(IndexedSeq(), 4) must_== IndexedSeq(
        bucket(), bucket(), bucket(), bucket())

      splitEvenly(IndexedSeq("a"), 4) must_== IndexedSeq(
        bucket("a"), bucket(), bucket(), bucket())

      splitEvenly(IndexedSeq("a", "b"), 4) must_== IndexedSeq(
        bucket("a"), bucket("b"), bucket(), bucket())

      splitEvenly(IndexedSeq("a", "b", "c"), 4) must_== IndexedSeq(
        bucket("a"), bucket("b"), bucket("c"), bucket())

      splitEvenly(IndexedSeq("a", "b", "c", "d"), 4) must_== IndexedSeq(
        bucket("a"), bucket("b"), bucket("c"), bucket("d"))

      splitEvenly(IndexedSeq("a", "b", "c", "d", "e"), 4) must_== IndexedSeq(
        bucket("a", "b"), bucket("c"), bucket("d"), bucket("e"))

      splitEvenly(IndexedSeq("a", "b", "c", "d", "e", "f"), 4) must_== IndexedSeq(
        bucket("a", "b"), bucket("c", "d"), bucket("e"), bucket("f"))

      splitEvenly(IndexedSeq("a", "b", "c", "d", "e", "f", "g"), 4) must_== IndexedSeq(
        bucket("a", "b"), bucket("c", "d"), bucket("e", "f"), bucket("g"))

      splitEvenly(IndexedSeq("a", "b", "c", "d", "e", "f", "g", "h"), 4) must_== IndexedSeq(
        bucket("a", "b"), bucket("c", "d"), bucket("e", "f"), bucket("g", "h"))

      splitEvenly(IndexedSeq("a", "b", "c", "d", "e", "f", "g", "h", "i"), 4) must_== IndexedSeq(
        bucket("a", "b", "c"), bucket("d", "e"), bucket("f", "g"), bucket("h", "i"))

    }

    "split correctly when excluding 0 sized buckets" in {
      // 1 bucket
      splitEvenly(IndexedSeq(), 1, excludeZeroSizeBuckets = true) must_== IndexedSeq()
      splitEvenly(IndexedSeq("a"), 1, excludeZeroSizeBuckets = true) must_== IndexedSeq(bucket("a"))
      splitEvenly(IndexedSeq("a", "b"), 1, excludeZeroSizeBuckets = true) must_== IndexedSeq(bucket("a", "b"))
      splitEvenly(IndexedSeq("a", "b", "c"), 1, excludeZeroSizeBuckets = true) must_== IndexedSeq(bucket("a", "b", "c"))

      // 2 buckets
      splitEvenly(IndexedSeq(), 2, excludeZeroSizeBuckets = true) must_== IndexedSeq()

      splitEvenly(IndexedSeq("a"), 2, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a"))

      splitEvenly(IndexedSeq("a", "b"), 2, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a"), bucket("b"))

      splitEvenly(IndexedSeq("a", "b", "c"), 2, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a", "b"), bucket("c"))

      splitEvenly(IndexedSeq("a", "b", "c", "d"), 2, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a", "b"), bucket("c", "d"))

      // 3 buckets
      splitEvenly(IndexedSeq(), 3, excludeZeroSizeBuckets = true) must_== IndexedSeq()

      splitEvenly(IndexedSeq("a"), 3, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a"))

      splitEvenly(IndexedSeq("a", "b"), 3, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a"), bucket("b"))

      splitEvenly(IndexedSeq("a", "b", "c"), 3, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a"), bucket("b"), bucket("c"))

      splitEvenly(IndexedSeq("a", "b", "c", "d"), 3, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a", "b"), bucket("c"), bucket("d"))

      splitEvenly(IndexedSeq("a", "b", "c", "d", "e"), 3, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a", "b"), bucket("c", "d"), bucket("e"))

      splitEvenly(IndexedSeq("a", "b", "c", "d", "e", "f"), 3, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a", "b"), bucket("c", "d"), bucket("e", "f"))

      splitEvenly(IndexedSeq("a", "b", "c", "d", "e", "f", "g"), 3, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a", "b", "c"), bucket("d", "e"), bucket("f", "g"))

      // 4 buckets
      splitEvenly(IndexedSeq(), 4, excludeZeroSizeBuckets = true) must_== IndexedSeq()

      splitEvenly(IndexedSeq("a"), 4, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a"))

      splitEvenly(IndexedSeq("a", "b"), 4, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a"), bucket("b"))

      splitEvenly(IndexedSeq("a", "b", "c"), 4, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a"), bucket("b"), bucket("c"))

      splitEvenly(IndexedSeq("a", "b", "c", "d"), 4, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a"), bucket("b"), bucket("c"), bucket("d"))

      splitEvenly(IndexedSeq("a", "b", "c", "d", "e"), 4, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a", "b"), bucket("c"), bucket("d"), bucket("e"))

      splitEvenly(IndexedSeq("a", "b", "c", "d", "e", "f"), 4, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a", "b"), bucket("c", "d"), bucket("e"), bucket("f"))

      splitEvenly(IndexedSeq("a", "b", "c", "d", "e", "f", "g"), 4, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a", "b"), bucket("c", "d"), bucket("e", "f"), bucket("g"))

      splitEvenly(IndexedSeq("a", "b", "c", "d", "e", "f", "g", "h"), 4, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a", "b"), bucket("c", "d"), bucket("e", "f"), bucket("g", "h"))

      splitEvenly(IndexedSeq("a", "b", "c", "d", "e", "f", "g", "h", "i"), 4, excludeZeroSizeBuckets = true) must_== IndexedSeq(
        bucket("a", "b", "c"), bucket("d", "e"), bucket("f", "g"), bucket("h", "i"))

    }
  }
}
