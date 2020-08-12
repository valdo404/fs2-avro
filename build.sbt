organization := "com.levaldo.fs2"
version := "1.0"
description := "Stream your avro files with fs2"
normalizedName := "avro"
scalaVersion := "2.13.3"

import Dependencies._

libraryDependencies ++= (fs2 ++ avro ++ specs2)

crossScalaVersions := Seq("2.12.10", "2.13.3")
