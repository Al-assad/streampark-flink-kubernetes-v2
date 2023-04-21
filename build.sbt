name         := "streampark-flink-kubernetes-v2"
version      := "0.1.0"
scalaVersion := "3.2.2"

libraryDependencies ++= Seq(
  "org.apache.flink"               % "flink-kubernetes-operator-api" % "1.4.0" exclude ("io.fabric8", "kubernetes-client"),
  "io.fabric8"                     % "kubernetes-client"             % "6.5.1",
  "dev.zio"                       %% "zio-streams"                   % "2.0.13",
  "dev.zio"                       %% "zio-concurrent"                % "2.0.13",
  "com.softwaremill.sttp.client3" %% "zio"                           % "3.8.13",
  "com.softwaremill.sttp.client3" %% "zio-json"                      % "3.8.13",
  "com.lihaoyi"                   %% "os-lib"                        % "0.9.1",
  "com.lihaoyi"                   %% "pprint"                        % "0.8.1",
  "com.lihaoyi"                   %% "upickle"                       % "3.0.0",
  "com.typesafe.scala-logging"    %% "scala-logging"                 % "3.9.5",
)
