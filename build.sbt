
val elastic4sVersion = "6.5.1"

val commonSettings = Seq(
  organization    := "sghaida",
  scalaVersion    := "2.12.8",
  version         := "0.0.1",

  resolvers ++= Seq(
    "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Artima Maven Repository" at "http://repo.artima.com/releases"
  ),

  libraryDependencies ++= Seq(
    "org.apache.kafka" % "kafka-clients" % "2.1.1",
    "org.apache.kafka" %% "kafka-streams-scala" % "2.1.1",
    "org.slf4j" % "slf4j-simple" % "1.7.26",
    "org.twitter4j" % "twitter4j-core" % "4.0.7",
    "com.twitter" % "hbc-core" % "2.2.0",
    "org.json4s" %% "json4s-jackson" % "3.6.5",

    // elasticsearch
    "com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion,
    "com.sksamuel.elastic4s" %% "elastic4s-http" % elastic4sVersion,
    "com.sksamuel.elastic4s" %% "elastic4s-http-streams" % elastic4sVersion,
    "com.sksamuel.elastic4s" %% "elastic4s-testkit" % elastic4sVersion % "test",
    "com.sksamuel.elastic4s" %% "elastic4s-embedded" % elastic4sVersion % "test",

    "org.scalactic" %% "scalactic" % "3.0.5",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  ),

  scalacOptions ++= Seq(
    "-feature",
    "-language:implicitConversions",
    "-language:postfixOps"
  )
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*).settings(
  name := "kafka-examples",
  mainClass in assembly := Some("com.sghaida.streams.WordCount")
)