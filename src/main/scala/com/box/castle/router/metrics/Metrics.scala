package com.box.castle.router.metrics

object Metrics {
  // These are non fatal failures
  val UnexpectedFailures      = "unexpected_failures"
  val CacheHits               = "cache_hits"
  val BatchedRequests         = "batched_requests"
  val AvoidedFetches          = "avoided_fetches"
  val BytesFetched            = "bytes_fetched"
  val NumFetches              = "fetches"
  val InvalidMessages         = "invalid_messages"
  val UnknownTopicOrPartition = "unknown_topic_or_partition_responses"
}
