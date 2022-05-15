package com.experiments.sam

import java.time.LocalDateTime
import java.util.UUID

import scala.concurrent.duration._

import cats.Monad
import cats.effect.kernel.{Async, Sync}
import cats.effect.{IO, IOApp}
import cats.syntax.all._
import com.comcast.ip4s._
import fs2.Stream
import fs2.kafka._
import io.circe.syntax._
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.server._
import org.http4s.implicits._
import org.http4s.server.Server

import Kafka._
object Main extends IOApp.Simple {

  override def run: IO[Unit] =
    AppResources
      .make[IO]
      .flatMap { settings =>
        Stream(
          KafkaProducer
            .stream(settings.kafka) -> Stream.emit(settings.server)
        )
      }
      .flatMap { case (kafka, server) =>
        Stream(
          server,
          kafka.flatMap { producer =>
            val kafka = Kafka
              .make(producer)
            val messages = Stream(
              Message(
                Map(
                  "uuid"    -> UUID.randomUUID().toString,
                  "name"    -> "Samuel",
                  "surname" -> "Gomez",
                  "email"   -> "samgomjim.18@gmail.com"
                )
              )
            ).repeatN(35)

            messages
              .evalMap(msg => kafka.publish(msg, "topic"))
              .groupWithin(512, 5.seconds)
              .evalMap(_.sequence)
              .unchunks
              .evalTap(e => IO.println(s"published $e"))
              .void
          }
        ).parJoinUnbounded
      }
      .compile
      .resource
      .lastOrError
      .useForever
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
  val kafka: KafkaService[F],
  val server: Server
)

object AppResources {
  def make[F[_]: Async]: Stream[F, AppResources[F]] = {
    def makeKafka(config: KafkaConfig): Stream[F, ProducerSettings[F, String, Message]] = Stream(
      ProducerSettings(
        keySerializer = Serializer[F, String],
        valueSerializer = Serializer.instance[F, Message] { (_, _, kafkaReq) =>
          Sync[F].delay(kafkaReq.data.asJson.toString().getBytes("UTF-8"))
        }
      ).withBootstrapServers(config.brokers)
    ).covary[F]

    (makeKafka(KafkaConfig("localhost:9092", "topic")), MkHttpServer[F].make).mapN(new AppResources(_, _) {})
  }
}

trait MkHttpServer[F[_]] {
  def make: Stream[F, Server]
}
object MkHttpServer {

  // Routes
  final case class Routes[F[_]: Monad]() extends Http4sDsl[F] {
    private val httpRoutes: HttpRoutes[F] = HttpRoutes.of[F] { case GET -> Root =>
      Ok("brands.findAll")
    }
    val routes = httpRoutes
  }

  def apply[F[_]: MkHttpServer]: MkHttpServer[F] = implicitly[MkHttpServer[F]]

  implicit def forAsync[F[_]: Async]: MkHttpServer[F] = new MkHttpServer[F] {
    override def make: Stream[F, Server] =
      Stream
        .resource(
          EmberServerBuilder
            .default[F]
            .withHost(host"0.0.0.0")
            .withPort(port"8080")
            .withHttpApp(Routes[F].routes.orNotFound)
            .build
        )
  }
}
