
### 1. Compile
In directory dataanalysis

sbt compile

### 2. Package

In directory dataanalysis

sbt package

### 3. Jar copy

copy jar from /dataanalysis/target/scala-2.11/data-project_2.11-1.0.jar  into data folder with docker spark setup utilizing same location as /tmp/data

### 4. Run code

In the docker execute

spark-submit --class "analysis" --master "local" /tmp/data/data-project_2.11-1.0.jar
