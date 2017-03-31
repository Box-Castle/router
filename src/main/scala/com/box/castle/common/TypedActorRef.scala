package com.box.castle.common

import akka.actor.{Actor, ActorRef}

import scala.language.implicitConversions

/**
  * This is a wrapper class for ActorRefs to provide some degree of type safety when passing
  * around arbitrary ActorRefs.  It also facilitates mocking as I had some issues mocking ActorRefs
  * directly in the past.
  *
  */
abstract class TypedActorRef {
  val ref: ActorRef
  def !(message: Any)(implicit sender: ActorRef = Actor.noSender): Unit =
    ref ! message
}

object TypedActorRef {
  implicit def convertToActorRef(typedActorRef: TypedActorRef): ActorRef =
    typedActorRef.ref
}
