package com.box.castle.router.kafkadispatcher.processors

import akka.actor.ActorRef
import com.box.castle.common.TypedActorRef

/**
 * Created by dgrenader on 6/4/15.
 */
case class RequesterInfo(ref: ActorRef) extends TypedActorRef