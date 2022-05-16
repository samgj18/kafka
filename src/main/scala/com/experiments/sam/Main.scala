package com.experiments.sam

import java.time.LocalDateTime
import java.util.UUID

import scala.util.control.NoStackTrace

import cats.effect.kernel.{Async, Sync}
import cats.effect.{IO, IOApp}
import cats.syntax.all._
import com.comcast.ip4s._
import com.experiments.sam.Kafka._
import fs2.Stream
import fs2.kafka._
import io.circe.generic.semiauto._
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.{Decoder, KeyDecoder}
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.server._
import org.http4s.implicits._
import org.http4s.server.Server
import io.circe.HCursor

object Main extends IOApp.Simple {

  override def run: IO[Unit] = {
    val config = AppResources.make[IO]
    val program: Stream[IO, Server] =
      for {
        producer <- KafkaProducer.stream(config.producer)
        consumer <- KafkaConsumer.stream(config.consumer).subscribeTo("topic")
        kafka    <- Stream(Kafka.make(producer, consumer))
        server   <- MkHttpServer[IO].make(kafka).evalTap(_ => IO.never)
      } yield server

    program.compile.drain
  }
}

trait Kafka[F[_]] {
  def publish(msg: Message, topic: String): F[F[Result]]
  def read: Stream[F, String]
}

object Kafka {
  type Producer[F[_]]             = KafkaProducer[F, String, Message]
  type Consumer[F[_]]             = KafkaConsumer[F, String, Message]
  type Result                     = ProducerResult[Unit, String, Message]
  type KafkaProducerService[F[_]] = ProducerSettings[F, String, Message]
  type KafkaConsumerService[F[_]] = ConsumerSettings[F, String, Message]

  case class Message(data: Map[String, String])

  object Message {
    implicit val decoder: Decoder[Message] = new Decoder[Message] {
      final def apply(c: HCursor): Decoder.Result[Message] =
        for {
          name    <- c.downField("name").as[String]
          source  <- c.downField("source").as[String]
          email   <- c.downField("email").as[String]
          uuid    <- c.downField("uuid").as[String]
          surname <- c.downField("surname").as[String]
        } yield new Message(
          data = Map("name" -> name, "source" -> source, "email" -> email, "uuid" -> uuid, "surname" -> surname)
        )
    }
  }

  case class DecodingError(reason: String) extends NoStackTrace

  final case class GenericMessage[K, V](data: Map[K, V])

  object GenericMessage {
    implicit def decoderForGenericMessage[K: KeyDecoder, V: Decoder]: Decoder[GenericMessage[K, V]] =
      deriveDecoder[GenericMessage[K, V]]
  }

  case class KafkaConfig(brokers: String, topic: String)

  def make[F[_]: Async](producer: Producer[F], consumer: Consumer[F]) =
    new Kafka[F] {
      override def publish(msg: Message, topic: String): F[F[Result]] = {
        val key    = LocalDateTime.now.toString()
        val record = ProducerRecord(topic, key, msg)
        producer.produce(ProducerRecords.one(record))
      }

      override def read =
        consumer.records.mapAsync(2) { commitable =>
          Async[F].delay(s"Processing record: ${commitable.record}")
        }
    }
}

sealed abstract case class AppResources[F[_]] private (
  val producer: KafkaProducerService[F],
  val consumer: KafkaConsumerService[F]
)

object AppResources {
  import Kafka._

  def make[F[_]: Async]: AppResources[F] = {
    def makeKafka(config: KafkaConfig) = {
      lazy val kafkaProducer: ProducerSettings[F, String, Message] =
        ProducerSettings(
          keySerializer = Serializer[F, String],
          valueSerializer = Serializer.instance[F, Message] { (_, _, kafkaReq) =>
            Sync[F].delay(kafkaReq.data.asJson.toString().getBytes("UTF-8"))
          }
        ).withBootstrapServers(config.brokers)
      lazy val kafkaConsumer: ConsumerSettings[F, String, Message] =
        ConsumerSettings(
          keyDeserializer = Deserializer[F, String],
          valueDeserializer = Deserializer.instance[F, Message] { (_, _, kafkaReq) =>
            decode[Message](new String(kafkaReq, "UTF-8")) match {
              case Left(value)  => DecodingError(value.getMessage).raiseError[F, Message]
              case Right(value) => value.pure[F]
            }
          }
        ).withBootstrapServers(config.brokers)
          .withGroupId("readers")
          .withAutoOffsetReset(AutoOffsetReset.Earliest)

      (kafkaProducer, kafkaConsumer)

    }

    val (producer, consumer) = makeKafka(KafkaConfig("localhost:9092", "topic"))

    new AppResources[F](producer, consumer) {}
  }
}

trait MkHttpServer[F[_]] {
  def make(kafka: Kafka[F]): Stream[F, Server]
}
object MkHttpServer {

  // Routes
  final case class Routes[F[_]: Sync](kafka: Kafka[F]) extends Http4sDsl[F] {
    private val genUUID: F[UUID] = Sync[F].delay(UUID.randomUUID())

    private def mkMessage(id: UUID) =
      Message(
        Map(
          "uuid"    -> id.toString,
          "name"    -> "Samuel",
          "surname" -> "Gomez",
          "email"   -> "samgomjim.18@gmail.com",
          "source"  -> "http4s"
        )
      )

    private val httpRoutes: HttpRoutes[F] = HttpRoutes.of[F] {
      case GET -> Root =>
        for {
          id       <- genUUID
          _        <- kafka.publish(mkMessage(id), "topic")
          response <- Ok(id.toString)
        } yield response

      case GET -> Root / "read" =>
        Ok(kafka.read)
    }

    val routes: HttpRoutes[F] = httpRoutes
  }

  def apply[F[_]: MkHttpServer]: MkHttpServer[F] = implicitly[MkHttpServer[F]]

  implicit def forAsync[F[_]: Async]: MkHttpServer[F] = new MkHttpServer[F] {
    override def make(kafka: Kafka[F]): Stream[F, Server] =
      Stream
        .resource(
          EmberServerBuilder
            .default[F]
            .withHost(host"0.0.0.0")
            .withPort(port"8080")
            .withHttpApp(Routes[F](kafka).routes.orNotFound)
            .build
        )
  }
}
