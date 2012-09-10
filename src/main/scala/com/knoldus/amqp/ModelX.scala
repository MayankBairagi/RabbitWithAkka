package com.knoldus.amqp
import com.rabbitmq.client.QueueingConsumer
import com.rabbitmq.client.ConnectionFactory
import scala.io.Source

// *****TODO*****Impliment Model Trait With ProducerFact and ConsumerFact Instead of two traits
case class Connection(host: String, port: Int, userName: String, password: String)

case class ConnectionProperties(exchangeName: String, queueName: String,routingKey:String, durable: Boolean)

trait RabbitFactory{
  var counter=0
  val initial=100000
  var startTime=0.0
   def createConsumer(connection:Connection,properties:ConnectionProperties)={
    val channel = createChannel(connection)
    channel.exchangeDeclare(properties.exchangeName, "direct", properties.durable)
    channel.queueDeclare(properties.queueName, properties.durable, false, false, null)
    channel.queueBind(properties.queueName, properties.exchangeName, properties.routingKey)
    val consumer = new QueueingConsumer(channel)
    channel.basicConsume(properties.queueName,true,consumer)
    consumer
  }
     
   def createChannel(connection:Connection)={
        		val connectionFactory = new ConnectionFactory
        		connectionFactory.setUsername(connection.userName)
        		connectionFactory.setPassword(connection.password)
        		connectionFactory.setVirtualHost("/")
        		connectionFactory.setHost(connection.host)
        		connectionFactory.setPort(connection.port)
        		val conn = connectionFactory.newConnection
        		val channel = conn.createChannel
        		channel
     	}

  def getThroughput={

    if (counter == initial) startTime = System.currentTimeMillis
    if (counter == (initial+100000)) {
      val elapsedTime = System.currentTimeMillis - startTime
      val throughput = 100000.toDouble * 1000.0 / elapsedTime
      println("Elapsed Time millis: " + elapsedTime)
      println("Throughput msgs/sec: " + throughput)
    }
     //println("counter "+counter)
    counter = counter + 1
  }
}




  