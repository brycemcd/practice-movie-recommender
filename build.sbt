lazy val root = (project in file(".")).
settings(
  name := "newProj",
  version := "0.0.1",
  scalaVersion := "2.11.7",
  mainClass := Some("Main"),

  libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0",
  libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.0",

  // https://github.com/databricks/learning-spark/blob/master/build.sbt
  resolvers ++= Seq(
    "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
    "Spray Repository" at "http://repo.spray.cc/",
    "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    "Akka Repository" at "http://repo.akka.io/releases/",
    "Twitter4J Repository" at "http://twitter4j.org/maven2/",
    "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
    "Twitter Maven Repo" at "http://maven.twttr.com/",
    "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
    "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
    "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
    Resolver.sonatypeRepo("public")
  )
)
