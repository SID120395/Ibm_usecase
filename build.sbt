
/*
name := "Ibm_test"

version := "0.1"

scalaVersion := "2.13.5"
*/
name := "Ibm_test"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "com.ibm.stocator" % "stocator" % "1.0.24",
  "com.ibm.ibmos2spark" %% "ibmos2spark" % "0.0.9",
  //"com.ibm.db2.jcc" %% "db2jcc" % "db2jcc4",
  "mysql" % "mysql-connector-java" % "8.0.20",
  //"com.ibm.db2" % "jcc" % "11.5.4.0"
)