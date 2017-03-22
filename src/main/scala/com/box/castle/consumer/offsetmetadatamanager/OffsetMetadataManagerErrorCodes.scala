package com.box.castle.consumer.offsetmetadatamanager

// $COVERAGE-OFF$
object OffsetMetadataManagerErrorCodes {
  // 0 - Success
  val NoError: Short = 0

  // 20100 - Cannot connect to OffsetMetadataManager persistent store
  val CannotConnectToPersistentStore: Short = 20100

  // 20200 - Invalid Offset Metadata Key, if persistent store is Zk, ensure the path is valid
  val InvalidOffsetMetadataKey: Short       = 20200

  // 20200 - OffsetMetadata Value size too large
  val OffsetMetadataValueTooLarge: Short    = 20200

  // 20300 - Znode Does not exist
  val ZNodeDoesNotExist: Short              = 20300

  // 20999 - Unknown OffsetMetadataManager
  val UnknownError: Short = 20999

}
// $COVERAGE-ON$