ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "implicits-demo"
  )
ThisBuild / organization := "adam"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"

enablePlugins(DockerPlugin)

docker / dockerfile := {
  val jarFile: File = (Compile / packageBin / sbt.Keys.`package`).value
  val classpath = (Compile / managedClasspath).value
  val mainclass = (Compile / packageBin / mainClass).value.getOrElse(sys.error("Expected exactly one main class"))
  val jarTarget = s"/opt/spark/jars/${jarFile.getName}"
  val classpathString = classpath.files.map("/opt/spark/work-dir/" + _.getName)
    .mkString(":") + ":" + jarTarget
  ImageName(s"${organization.value}/${name.value}:latest")
  new Dockerfile {
    from("spark:v3.2.1")
    add(jarFile, jarTarget)
    user("root")
   // entryPoint("/opt/spark/bin/spark-submit", "--conf", "spark.jars.ivy=/tmp/.ivy", jarTarget)
  }
}