package edu.rohan.kafka

import edu.rohan.kafka.KafkaStreams.Domain.{Discount, UserId}
import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.ImplicitConversions._

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties

object KafkaStreams {

  object Domain {
    type UserId = String
    type Profile = String
    type Product = String
    type OrderId = String
    type Status = String

    case class Order(orderId: OrderId, userId: UserId, products: List[Product], amount: Double)

    case class Discount(profile: Profile, amount: Double)

    case class Payment(orderId: OrderId, status: Status)
  }

  object Topic {
    val OrdersByUser = "orders-by-user"
    val DiscountProfilesByUser = "discount-profiles-by-user"
    val Discounts = "discounts"
    val Orders = "orders"
    val Payments = "payments"
    val PaidOrders = "paid-orders"
  }

  // source = emits elements
  // flow = transforms elements along the way ( e.g. map)
  // sink = "ingests" elements

  import Domain._
  import Topic._

  implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
    val serializer = (a: A) => a.asJson.noSpaces.getBytes()
    val deserializer = (aAsBytes: Array[Byte]) => {
      val string = new String(aAsBytes)
      val decodedValue = decode[A](string).toOption
      decodedValue
    }
    Serdes.fromFn[A](serializer, deserializer)
  }


  def main(args: Array[String]): Unit = {
    // topology
    val builder = new StreamsBuilder()

    // KStreams
    val usersOrdersStream: KStream[UserId, Order] = builder.stream[UserId, Order](OrdersByUser)

    // KTables -- distributed to all nodes
    val userProfilesTable: KTable[UserId, Profile] = builder.table[UserId, Profile](DiscountProfilesByUser)

    // Global KTables -- copied to all the nodes
    val discountProfilesGTable: GlobalKTable[Profile, Discount] = builder.globalTable[Profile, Discount](Discounts)

    // KStreams Transformations: filter,map,flatMap,mapValues
    val expensiveOrders = usersOrdersStream.filter { (userId, order) =>
      order.amount > 1000
    }

    val listOfProduct = usersOrdersStream.mapValues(order => order.products)

    val products = usersOrdersStream.flatMapValues(_.products)

    // Join Dynamic (Stream) vs Static (Table)
    val orderWithUserProfiles = usersOrdersStream.join(userProfilesTable) { (order, profile) => (order, profile) }

    val discountedOrderStream = orderWithUserProfiles.join(discountProfilesGTable)(
      { case (userId, (order, profile)) => profile }, //key of the join, picked from the "left" stream
      { case ((order, profile), discount) => order.copy(amount = order.amount - discount.amount) } //values of the matched records
    )

    // pick another identifier
    val orderStream = discountedOrderStream.selectKey((userId, order) => order.orderId)
    val paymentStream = builder.stream[OrderId, Payment](Payments)

    val joinWindow = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))

    val joinOrdersPayments = (order: Order, payment: Payment) => if (payment.status == "PAID") Option(order) else Option.empty[Order]
    val ordersPaid = orderStream.join(paymentStream)(joinOrdersPayments, joinWindow)
      .flatMapValues(maybePaidOrder => maybePaidOrder.toSeq)

    //sink
    ordersPaid.to(PaidOrders)

    val topology = builder.build()

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

    println(topology.describe())

    val application = new KafkaStreams(topology, props)
    application.start()
  }
}
