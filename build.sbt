name := "tailor"

version := "0.1"

scalaVersion := "2.11.12"

addCompilerPlugin("org.psywerx.hairyfotr" %% "linter" % "0.1.17")

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings")

scalastyleFailOnWarning := true

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
