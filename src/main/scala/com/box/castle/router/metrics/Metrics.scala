package com.box.castle.router.metrics

object Metrics {
  // These are non fatal failures
  val UnexpectedFailures      = "unexpected_failures"
  val CacheHits               = "cache_hits"
  val BatchedRequests         = "batched_requests"
  val AvoidedFetches          = "avoided_fetches"
  val BytesFetched            = "bytes_fetched"
  val NumFetches              = "num_fetches"
  val InvalidMessages         = "num_invalid_messages"
  val UnknownTopicOrPartition = "num_unknown_topic_or_partition_responses"
}
