package com.box.castle.router

import akka.actor.ActorRef
import com.box.castle.common.TypedActorRef

case class RouterRef(ref: ActorRef) extends TypedActorRef