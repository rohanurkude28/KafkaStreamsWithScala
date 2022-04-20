package edu.rohan.kafka

import edu.rohan.kafka.KafkaStreams.Topic
import org.apache.kafka.streams.StreamsConfig

object KafkaProducerExample {
  def main(args: Array[String]): Unit = {
    import java.util.Properties

    import org.apache.kafka.clients.producer._

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    import Topic._
    var i=1

    //TODO: Check why we have to send two messages for one to get consumed
    while(i<3) {
      val discountRecord1 = new ProducerRecord(Discounts, "profile1", "{\"profile\":\"profile1\",\"amount\":10 }")
      producer.send(discountRecord1)

      val discountRecord2 = new ProducerRecord(Discounts, "profile2", "{\"profile\":\"profile2\",\"amount\":100 }")
      producer.send(discountRecord2)

      val discountRecord3 = new ProducerRecord(Discounts, "profile3", "{\"profile\":\"profile3\",\"amount\":40 }")
      producer.send(discountRecord3)

      val discountByProfileRecord1 = new ProducerRecord(DiscountProfilesByUser, "Daniel", "profile1")
      producer.send(discountByProfileRecord1)

      val discountByProfileRecord2 = new ProducerRecord(DiscountProfilesByUser, "Riccardo", "profile2")
      producer.send(discountByProfileRecord2)

      val orderByUserRecord1 = new ProducerRecord(OrdersByUser, "Daniel", "{\"orderId\":\"order1\",\"userId\":\"Daniel\",\"products\":[ \"iPhone 13\",\"MacBook Pro 15\"],\"amount\":4000.0 }")
      producer.send(orderByUserRecord1)

      val orderByUserRecord2 = new ProducerRecord(OrdersByUser, "Riccardo", "{\"orderId\":\"order2\",\"userId\":\"Riccardo\",\"products\":[\"iPhone 11\"],\"amount\":800.0}")
      producer.send(orderByUserRecord2)

      val orderRecord1 = new ProducerRecord(Orders, "order1", "{\"orderId\":\"order1\",\"status\":\"PAID\"}")
      producer.send(orderRecord1)

      val orderRecord2 = new ProducerRecord(Orders, "order2", "{\"orderId\":\"order2\",\"status\":\"PENDING\"}")
      producer.send(orderRecord2)

      i+=1
    }
    producer.close()
  }
}
