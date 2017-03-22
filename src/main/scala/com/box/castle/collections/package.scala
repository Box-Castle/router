package com.box.castle

package object collections {

  /**
    * Splits the given sequence into K buckets as evenly as possible
    * @param seq - the sequence to split
    * @param numBuckets - the number of buckets to split elements into, must be > 0
    * @param excludeZeroSizeBuckets - if this is set to true, buckets of size 0 will not be returned and the size of the
    *                                 returned IndexedSeq might be less than the num buckets specified.
    *                                 if this is set to false, buckets of size 0 will be returned and the size of the
    *                                 returned IndexedSeq will be exactly the same as the num buckets specified.
    * @return - an IndexedSeq which specifies the number of elements in each of the buckets
    */
  def splitEvenly[A](
      seq: IndexedSeq[A],
      numBuckets: Int,
      excludeZeroSizeBuckets: Boolean = false): IndexedSeq[IndexedSeq[A]] = {
    require(seq.size >= 0)
    require(numBuckets > 0)
    val baseNumElementsPerBucket = seq.size / numBuckets
    val remainingElements = seq.size % numBuckets
    val b = IndexedSeq.newBuilder[IndexedSeq[A]]
    b.sizeHint(numBuckets)
    var from = 0
    var i = 0
    while (i < numBuckets) {
      val numElementsInBucket =
        if (i < remainingElements) baseNumElementsPerBucket + 1
        else baseNumElementsPerBucket

      if (!excludeZeroSizeBuckets || numElementsInBucket > 0)
        b += seq.slice(from, from + numElementsInBucket)

      from += numElementsInBucket
      i += 1
    }
    b.result()
  }
}
