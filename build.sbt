name := """ledger-wallet-daemon"""
version := "2.0.0"

lazy val commonSettings = Seq(
  organization := "co.ledger",
  scalaVersion := "2.12.2"
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .aggregate(binding, daemon)

lazy val binding = (project in file("ledger-core-binding"))
  .settings(commonSettings)

lazy val daemon = (project in file("daemon"))
  .settings(commonSettings)
  .enablePlugins(JavaServerAppPackaging)
  .dependsOn(binding)
