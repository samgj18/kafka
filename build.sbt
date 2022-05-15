ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

ThisBuild / scalafixDependencies += "com.github.liancheng" %% "organize-imports" % "0.6.0"

ThisBuild / semanticdbEnabled := true

ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

ThisBuild / scalacOptions ++= List("-Ymacro-annotations", "-Yrangepos", "-Wconf:cat=unused:info", "-Wunused")

lazy val root = (project in file("."))
  .settings(
    name := "scala-kafka"
  )

libraryDependencies ++= Seq(
  "com.github.fd4s" %% "fs2-kafka"           % "2.5.0-M3",
  "org.typelevel"   %% "cats-effect"         % "3.3.11",
  "org.slf4j"        % "slf4j-simple"        % "1.7.12",
  "io.circe"        %% "circe-core"          % "0.14.1",
  "io.circe"        %% "circe-generic"       % "0.14.1",
  "io.circe"        %% "circe-refined"       % "0.14.1",
  "io.circe"        %% "circe-parser"        % "0.14.1",
  "org.http4s"      %% "http4s-dsl"          % "0.23.11",
  "org.http4s"      %% "http4s-ember-server" % "0.23.11",
  "org.http4s"      %% "http4s-ember-client" % "0.23.11"
)

addCommandAlias("runLinter", ";scalafixAll --rules OrganizeImports")
