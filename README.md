# sampleExercise


## Preparing the data

### Dataset

    ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/

### Retrieve daily summaries of weather data

    mkdir data
    cd data
    wget ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/2015.csv.gz

Use `wget` to retrieve [daily summaries](ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/) of weather data from NOAA's FTP. Unzip `*.csv.gz` files and put them into `data/`.

## Cassandra table schema

    CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
    CREATE TABLE test.weather(loc varchar,  date timestamp, tmax int, tmin int, PRIMARY KEY (loc, date)) with clustering order by (date asc);

## Build

    sbt/sbt assembly 

## Test

    sbt/sbt test
 
## Run

The main class is `org.viirya.weather.SparkApp` which can be used to insert daily summaries of weather data into Cassandra database and perform simple analysis.

### Insert weather data into Cassandra

    ./bin/spark-submit --class org.viirya.weather.SparkApp --master yarn-client --executor-memory 4G --num-executors 4 ../excercise/target/scala-2.10/BigDataExcercise-assembly-1.0.jar [Cassandra host] [keyspace] [table] insert [directory to data files]

### Calculate average maximum and minimum temperature for a specified year

    ./bin/spark-submit --class org.viirya.weather.SparkApp --master yarn-client --executor-memory 4G --num-executors 4 ../excercise/target/scala-2.10/BigDataExcercise-assembly-1.0.jar [Cassandra host] [keyspace] [table] analysis [the year to analyse]

