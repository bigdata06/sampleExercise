name := "BigDataExcercise"

version := "1.0"

scalaVersion := "2.11.4"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-feature")

libraryDependencies ++= {
  Seq(
    "org.scalatest"                 %%  "scalatest"       % "2.2.5"       % "test"
  )
}
