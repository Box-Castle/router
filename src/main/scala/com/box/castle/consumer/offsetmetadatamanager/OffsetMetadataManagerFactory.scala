package com.box.castle.consumer.offsetmetadatamanager

import kafka.consumer.SimpleConsumer

// $COVERAGE-OFF$
trait OffsetMetadataManagerFactory {

  def create(consumer: SimpleConsumer): OffsetMetadataManager

}
// $COVERAGE-ON$
