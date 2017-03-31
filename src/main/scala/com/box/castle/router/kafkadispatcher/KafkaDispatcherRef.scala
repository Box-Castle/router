package com.box.castle.router.kafkadispatcher

import akka.actor.ActorRef
import com.box.castle.common.TypedActorRef

case class KafkaDispatcherRef(ref: ActorRef) extends TypedActorRef
