package edu.rohan.kafka

import edu.rohan.kafka.KafkaStreams.Topic
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.streams.StreamsConfig

import java.util.Properties

object KafkaConsumerExample {

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "CLMB")

    val consumer = new KafkaConsumer[String, String](props)

    import Topic._

    import scala.jdk.CollectionConverters._
    val listOfTopics = List(OrdersByUser,DiscountProfilesByUser,Discounts,Orders,Payments,PaidOrders).asJavaCollection
    //val listOfTopics = List(PaidOrders).asJavaCollection
    consumer.subscribe(listOfTopics)

    while(true){
      val records=consumer.poll(100).asScala

      records.foreach(x => println(x.key()+" "+x.value()))
    }
    consumer.commitAsync()
}
}