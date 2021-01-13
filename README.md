# AWS GLUE LOCAL SCALA

This is a tool to develop and test AWS Glue scripts written in Scala. It uses SBT to manage the necessary resources for local testing. It was inspired by the documentation on locally testing Glue here: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html which suggests local installations of packages and running with Maven.

### Dependencies
* Java 8 - Later versions of Java will not work with AWS Glue
* SBT Version 1.3.10 - get it here https://www.scala-sbt.org/index.html
* Scala 2.11.1 or later

### Setup
Compile the package:  
```
sbt clean
sbt compile
```
Run the test example:  
```
sbt test
```
Or run directly:
```
sbt run --env local
```
