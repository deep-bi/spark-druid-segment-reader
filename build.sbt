import sbt.Keys.excludeDependencies

ThisBuild / version := "0.2.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

val sparkVersion = "2.3.4"
val hadoopVersion = "2.7.2"
val druidVersion = "0.22.1"

lazy val root = (project in file("."))
  .settings(
    name := "spark-druid-segment-reader",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-unsafe" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-graphx" % sparkVersion % Provided,
      "com.google.inject" % "guice" % "4.2.3",
      "com.google.inject.extensions" % "guice-multibindings" % "4.2.3",
      "org.apache.druid" % "druid-server" % druidVersion,
      "org.apache.druid" % "druid-processing" % druidVersion,
      "org.apache.druid" % "druid-core" % druidVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided,
      "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % Provided,

      // tests
      "org.scalatest" %% "scalatest" % "3.2.12" % Test
    ),
    excludeDependencies ++= Seq(
      "commons-logging" % "commons-logging",
      "log4j" % "log4j",
      "multi-bindings" % "multi-bindings",
      "org-hyperic" % "org-hyperic"
    )
  )

assembly / assemblyMergeStrategy := {
  case PathList("module-info.class") => MergeStrategy.rename
  case PathList("javax", xs@_*) => MergeStrategy.first
  case PathList("META-INF", xs@_*) =>
    xs map {
      _.toLowerCase
    } match {
      case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil =>
        MergeStrategy.discard
      case ps@x :: xs if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case "jersey-module-version" :: xs => MergeStrategy.first
      case "sisu" :: xs => MergeStrategy.discard
      case "maven" :: xs => MergeStrategy.discard
      case "plexus" :: xs => MergeStrategy.discard
      case _ => MergeStrategy.discard
    } // Link: https://github.com/metamx/druid-spark-batch/blob/master/build.sbt
  case x => MergeStrategy.first
}

resolvers += "sigar" at "https://repository.mulesoft.org/nexus/content/repositories/public"

scalacOptions += "-target:jvm-1.8"
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("com.google.inject" -> "shaded.com.google.inject").inAll,
  ShadeRule.rename("org.roaringbitmap.**" -> "shaded.org.roaringbitmap.@1").inAll
)
