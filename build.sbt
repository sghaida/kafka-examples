
lazy val root = (project in file(".")).settings(
  inThisBuild(List(
    organization    := "sghaida",
    scalaVersion    := "2.12.8",
    version         := "0.0.1"
  )),

  name := "kafka-examples",

  resolvers ++= Seq(
    "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Artima Maven Repository" at "http://repo.artima.com/releases"
  ),

  libraryDependencies ++= Seq(
    "org.apache.kafka" % "kafka-clients" % "2.1.1",
    "org.apache.kafka" %% "kafka-streams-scala" % "2.1.1",
    "org.slf4j" % "slf4j-simple" % "1.7.26",
    "org.scalactic" %% "scalactic" % "3.0.5",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    "org.twitter4j" % "twitter4j-core" % "4.0.7",
    "com.twitter" % "hbc-core" % "2.2.0"
  ),

  scalacOptions ++= Seq(
    "-feature",
    "-language:implicitConversions",
    "-language:postfixOps"
  )

)