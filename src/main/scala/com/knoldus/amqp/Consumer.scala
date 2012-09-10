package com.knoldus.amqp
import com.rabbitmq.client.QueueingConsumer
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import xml.XML

case class Credentials(connection:Connection,properties:ConnectionProperties)
case class Send(message:Array[Byte])

class Consumer(val credentials:Credentials) extends Actor with RabbitFactory  {
  var consumer:QueueingConsumer=null;
  override def preStart = {
     consumer=createConsumer(credentials.connection,credentials.properties)
     println("I am Listening")
    }

    def receive ={
      case "WAITING" => {
        while (true) {
        	val delivery = consumer.nextDelivery()
        	val data = delivery.getBody
          getThroughput
          if (counter==100000)   {
            val xmlString = new String(data)
            var xml = XML.loadString(xmlString)
          }
        }   
      }
    }
}   
   

object StartConsumer extends App
{
  // Set Connection Credentials
  val connection1=Connection("localhost", 5672, "guest", "guest")
  val connectionProperties1=ConnectionProperties("myexchange1", "myqueue1","key1", false)
  val credentials_1= Credentials(connection1,connectionProperties1)
  //###########################################################################

  val system= ActorSystem("MySystem")
  val consumer1=system.actorOf(Props(new Consumer(credentials_1)))
  consumer1 ! "WAITING"
  consumer1 ! "WAITING"
}

