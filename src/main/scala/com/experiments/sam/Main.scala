package com.experiments.sam

import cats.effect.kernel.{Async, Sync}
import cats.effect.{IO, IOApp}
import cats.syntax.all._
import com.comcast.ip4s._
import com.experiments.sam.Kafka._
import fs2.Stream
import fs2.kafka._
import io.circe.syntax._
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.server._
import org.http4s.implicits._
import org.http4s.server.Server

import java.time.LocalDateTime
import java.util.UUID
object Main extends IOApp.Simple {

  override def run: IO[Unit] = {
    val config = AppResources.make[IO]
    val program: Stream[IO, Server] =
      for {
        producer <- KafkaProducer.stream(config.kafka)
        kafka    <- Stream(Kafka.make(producer))
        server   <- MkHttpServer[IO].make(kafka).evalTap(_ => IO.never)
      } yield server

    program.compile.drain
  }
}

trait Kafka[F[_]] {
  def publish(msg: Message, topic: String): F[F[Result]]
}

object Kafka {
  type Producer[F[_]]     = KafkaProducer[F, String, Message]
  type Result             = ProducerResult[Unit, String, Message]
  type KafkaService[F[_]] = ProducerSettings[F, String, Message]

  case class Message(data: Map[String, String])
  case class KafkaConfig(brokers: String, topic: String)

  def make[F[_]](producer: Producer[F]) =
    new Kafka[F] {
      override def publish(msg: Message, topic: String): F[F[Result]] = {
        val key    = LocalDateTime.now.toString()
        val record = ProducerRecord(topic, key, msg)
        producer.produce(ProducerRecords.one(record))
      }
    }

}

sealed abstract case class AppResources[F[_]] private (
  val kafka: KafkaService[F]
)

object AppResources {
  def make[F[_]: Async]: AppResources[F] = {
    def makeKafka(config: KafkaConfig): ProducerSettings[F, String, Message] =
      ProducerSettings(
        keySerializer = Serializer[F, String],
        valueSerializer = Serializer.instance[F, Message] { (_, _, kafkaReq) =>
          Sync[F].delay(kafkaReq.data.asJson.toString().getBytes("UTF-8"))
        }
      ).withBootstrapServers(config.brokers)

    new AppResources[F](makeKafka(KafkaConfig("localhost:9092", "topic"))) {}
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

    private val httpRoutes: HttpRoutes[F] = HttpRoutes.of[F] { case GET -> Root =>
      for {
        id       <- genUUID
        _        <- kafka.publish(mkMessage(id), "topic")
        response <- Ok(id.toString)
      } yield response
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
