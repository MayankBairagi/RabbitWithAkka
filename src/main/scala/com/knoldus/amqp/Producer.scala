package com.knoldus.amqp

import com.rabbitmq.client.{Channel, ConnectionFactory}
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.routing.RoundRobinRouter
import io.Source

class Producer(credentials: Credentials) extends Actor with RabbitFactory {
  var channel: Channel = _

  override def preStart = {
    channel = createChannel(credentials.connection)
    channel.exchangeDeclare(credentials.properties.exchangeName, "direct", credentials.properties.durable)

  }

  def receive = {
    case Send(message) => {
      channel.basicPublish(credentials.properties.exchangeName, credentials.properties.routingKey, null, message)
      getThroughput
    }
  }
}

object StartProducer extends App {
  // Set Connection Credentials
  val connection1=Connection("localhost", 5672, "guest", "guest")
  val connectionProperties1=ConnectionProperties("myexchange1", "myqueue1","key1", false)
  val credentials_1= Credentials(connection1,connectionProperties1)
  //###########################################################################

  val system = ActorSystem("MySystem2")
  val producer = system.actorOf(Props(new Producer(credentials_1)).withRouter(RoundRobinRouter(nrOfInstances = 100)))
  val data = Source.fromFile("src/main/resources/message-fib.xml").mkString.getBytes
  println("Start Producing")
  (1 to 300000) foreach {
    x => producer ! Send(data)
  }

}