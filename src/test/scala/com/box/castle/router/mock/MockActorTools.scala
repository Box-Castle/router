package com.box.castle.router.mock

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.specs2.specification.BeforeAfter



trait MockActorTools {

  trait actorSystem extends BeforeAfter {
    var initializedSystem = false
    lazy implicit val system: ActorSystem = {
      initializedSystem = true
      ActorSystem("EventSourceSpec")
    }

    override def before: Unit = {

    }

    override def after = {
      if (initializedSystem)
        TestKit.shutdownActorSystem(system)
    }
  }
}
