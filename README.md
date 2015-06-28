# sampleExercise

## Build

    sbt/sbt assembly 

## Test

    sbt/sbt test
 
## Run

The main class is `org.viirya.weather.SparkApp` which needs to specify Cassandra host address.

### Insert weather data into Cassandra

    ./bin/spark-submit --class org.viirya.weather.SparkApp --master yarn-client --executor-memory 4G --num-executors 4 ../excercise/target/scala-2.10/BigDataExcercise-assembly-1.0.jar [Cassandra host] [keyspace] [table] insert [directory to data files]

### Calculate average maximum and minimum temperature for a specified year

    ./bin/spark-submit --class org.viirya.weather.SparkApp --master yarn-client --executor-memory 4G --num-executors 4 ../excercise/target/scala-2.10/BigDataExcercise-assembly-1.0.jar [Cassandra host] [keyspace] [table] analysis [the year to analyse]

