ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "scala-kafka"
  )

libraryDependencies ++= Seq(
  "com.github.fd4s" %% "fs2-kafka"     % "2.5.0-M3",
  "org.typelevel"   %% "cats-effect"   % "3.3.11",
  "org.slf4j"        % "slf4j-simple"  % "1.7.12",
  "io.circe"        %% "circe-core"    % "0.14.1",
  "io.circe"        %% "circe-generic" % "0.14.1",
  "io.circe"        %% "circe-refined" % "0.14.1",
  "io.circe"        %% "circe-parser"  % "0.14.1"
)
