package com.box.castle.router.kafkadispatcher.processors

import akka.actor.ActorRef
import com.box.castle.common.TypedActorRef




case class RequesterInfo(ref: ActorRef) extends TypedActorRef