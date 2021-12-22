package com.cswg.testing

import akka.actor.{ Actor, ActorSystem, Props, ActorRef }
import org.slf4j.LoggerFactory

class HelloActor(friend: ActorRef) extends Actor {
  var logger = LoggerFactory.getLogger("Examples");  
  var count = 0;
  
  override def preStart {
      logger.debug("Pre Start HelloActor")
  }
  
  override def postStop {
      logger.debug("Post Stop HelloActor")
  }
  
  override def preRestart( reason: Throwable, message: Option[Any] ) = {
      logger.debug("Pre Restart HelloActor " + message.getOrElse("") + " - " + reason.getMessage )
      super.preRestart(reason, message)
  }
  
  override def postRestart( reason: Throwable ) = {
      logger.debug("Post Restart HelloActor " + reason.getMessage)
      super.postRestart(reason)
  }  
  
  def receive = {
      case "start" => count += 1 
                      friend.!("hello")      
                      logger.debug("Hello. Start Sending Messages")
      case "hello" =>   if ( count >= 20 ) { logger.debug("Stop Sending Messages" )
                                             sender.!("stop") 
                                             //context.stop(self) 
                                             }
                      else { count += 1 
                             logger.debug("Send again. Message No. " + count )     
                             sender.!("hello")
                            }
      case _ => logger.debug("Something wrong happened. Message is lost")
      //case _ => logger.debug("huh?")
    }
}

class HelloActorParam1( myName: String ) extends Actor {
  var logger = LoggerFactory.getLogger("Examples");  
  override def preStart {
      logger.debug("Pre Start HelloActorParam1")
  }
  
  override def postStop {
      logger.debug("Post Stop HelloActorParam1")
  }
  
  override def preRestart( reason: Throwable, message: Option[Any] ) = {
      logger.debug("Pre Restart HelloActorParam1 " + message.getOrElse("") + " - " + reason.getMessage )
      super.preRestart(reason, message)
  }
  
  override def postRestart( reason: Throwable ) = {
      logger.debug("Post Restart HelloActorParam1 " + reason.getMessage)
      super.postRestart(reason)
  }    
  def receive = {
      case "hello" => logger.debug(s"hello $myName back at you")
                      sender.!("hello")                      
      case "stop" => logger.debug(s"stop $myName")
                     //context.stop(self)
      case _ => logger.debug(s"huh? $myName. something wrong happened")
    }
}

object HelloActor {
  var logger = LoggerFactory.getLogger("Examples");  
  def main( args: Array[String] ) = {
    
    logger.debug("Main for HelloActor")

    // an Actor needs an ActorSystem  
    val system = ActorSystem("HelloSystem")
    
    // create and start the Actor
    val helloActorParam1 = system.actorOf(Props(new HelloActorParam1("helloActor") ), name = "helloActorParam1")
    val helloActor = system.actorOf(Props(new HelloActor(helloActorParam1)), name = "helloActor")
    
    // send the actor 2 messages 
    helloActor.!("start")
    /*helloActor.!("hello")
    helloActor.!("hello")
    helloActor.!("buenos dias")
    
    helloActorParam1.!("hello")
    helloActorParam1.!("buenos dias")
    */
    
    // shut down the system
    system.shutdown()
  }
}