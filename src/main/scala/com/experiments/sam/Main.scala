package com.experiments.sam

import java.time.LocalDateTime

import cats.Monad
import cats.effect.kernel.{Async, Sync}
import cats.effect.{IO, IOApp}
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

  override def run: IO[Unit] = {
    val program = for {
      resources <- AppResources.make[IO]
      producer  <- KafkaProducer.stream(resources.kafka)
      kafka     <- Stream(Kafka.make(producer))
      server    <- MkHttpServer[IO].make(kafka)
    } yield server

    program.compile.resource.lastOrError.useForever
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
  def make[F[_]: Async]: Stream[F, AppResources[F]] = {
    def makeKafka(config: KafkaConfig): Stream[F, ProducerSettings[F, String, Message]] = Stream(
      ProducerSettings(
        keySerializer = Serializer[F, String],
        valueSerializer = Serializer.instance[F, Message] { (_, _, kafkaReq) =>
          Sync[F].delay(kafkaReq.data.asJson.toString().getBytes("UTF-8"))
        }
      ).withBootstrapServers(config.brokers)
    ).covary[F]

    makeKafka(KafkaConfig("localhost:9092", "topic")).map(new AppResources(_) {})
  }
}

trait MkHttpServer[F[_]] {
  def make(kafka: Kafka[F]): Stream[F, Server]
}
object MkHttpServer {

  // Routes
  final case class Routes[F[_]: Monad](kafka: Kafka[F]) extends Http4sDsl[F] {
    private val httpRoutes: HttpRoutes[F] = HttpRoutes.of[F] { case GET -> Root =>
      /* kafka.publish(
        Message(
          Map(
            "uuid"    -> UUID.randomUUID().toString,
            "name"    -> "Samuel",
            "surname" -> "Gomez",
            "email"   -> "samgomjim.18@gmail.com",
            "source"  -> "http4s"
          )
        ),
        "topic"
      ) */
      Ok("Hello, world!")
    }
    val routes = httpRoutes
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
