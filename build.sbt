 // build.sbt

ThisBuild / scalaVersion := "2.12.15"
ThisBuild / organization := "com.example"


// Define the name of the project
name := "me_project"

// Specify library dependencies (add Spark or other dependencies here)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.1",
  "org.apache.spark" %% "spark-sql" % "3.3.1"
)

// Set up the assembly plugin for generating a single JAR
enablePlugins(AssemblyPlugin)

// Assembly JAR settings
assembly / assemblyJarName := "project_jars.jar"

// Prevent merging strategy conflicts
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

