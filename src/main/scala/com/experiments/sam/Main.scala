package com.experiments.sam

import cats.effect.{IO, IOApp}
import fs2.kafka._
import cats.syntax.all._
import scala.concurrent.duration._
import fs2.kafka._
import io.circe.generic.auto._
import io.circe.syntax._
import java.time.LocalDateTime
import cats.MonadThrow
import cats.effect.kernel.Async
import cats.effect.kernel.Sync
import cats.Applicative
import Kafka._

import cats.effect.kernel.Resource
import java.util.UUID
object Main extends IOApp.Simple {

  override def run: IO[Unit] =
    AppResources
      .make[IO]
      .map { resources =>
        KafkaProducer
          .stream(resources.kafka)

      }
      .use(
        _.evalMap { producer =>
          Kafka
            .make(producer)
            .publish(
              Message(
                Map(
                  "uuid"    -> UUID.randomUUID().toString,
                  "name"    -> "Samuel",
                  "surname" -> "Gomez",
                  "email"   -> "samgomjim.18@gmail.com"
                )
              ),
              "topic"
            )
        }.repeatN(10).compile.drain
      )
}

trait Kafka[F[_]] {
  def publish(msg: Message, topic: String): F[Result]
}

object Kafka {
  type Producer[F[_]]     = KafkaProducer[F, String, Message]
  type Result             = ProducerResult[Unit, String, Message]
  type KafkaService[F[_]] = ProducerSettings[F, String, Message]

  case class Message(data: Map[String, String])
  case class KafkaConfig(brokers: String, topic: String)

  def make[F[_]: Sync](producer: Producer[F]) =
    new Kafka[F] {
      override def publish(msg: Message, topic: String): F[Result] = {
        val key    = LocalDateTime.now.toString()
        val record = ProducerRecord(topic, key, msg)
        producer.produce(ProducerRecords.one(record)).flatten
      }
    }

}

sealed abstract case class AppResources[F[_]] private (
  val kafka: KafkaService[F]
)

object AppResources {
  def make[F[_]: Sync]: Resource[F, AppResources[F]] = {
    def makeKafka(config: KafkaConfig): Resource[F, KafkaService[F]] = Resource.pure[F, KafkaService[F]](
      ProducerSettings(
        keySerializer = Serializer[F, String],
        valueSerializer = Serializer.instance[F, Message] { (topic, headers, kafkaReq) =>
          Sync[F].delay(kafkaReq.data.asJson.toString().getBytes("UTF-8"))
        }
      ).withBootstrapServers(config.brokers)
    )

    makeKafka(KafkaConfig("localhost:9092", "topic")).map(new AppResources(_) {})
  }
}
