# SparkProject

Places recommendations app. Details are described in description.docx file

## Prerequisites

Project requires Java 1.8

## Getting Started

To get this project just clone it with
```
git clone https://github.com/SAmser93/SparkProject
```

## Installing

To build .jar package use "clean package" command in maven

```mvn clean package```

## Run
First, you will need to generate set of random persona locations. To do put places_of_interest.csv file somewhere and 
run this jar as

```java -Dlog4j.configuration=log4j2.xml -Dpoi.csv=*path_to_places_of_interest.csv* -Dpl.csv=*persona_locations.csv* -cp .\spark-places-recommender-jar-with-dependencies.jar ru.sem.apache_spark_test.LocationsGenerator```

Then run main app as

```java -Dlog4j.configuration=log4j2.xml -Dpoi.csv=*path_to_places_of_interest.csv* -Dpl.csv=*persona_locations.csv* -Dres.csv=*final result file path* -cp .\spark-places-recommender-jar-with-dependencies.jar ru.sem.apache_spark_test.InMemorySpark```

To run with spark-submit use

```spark-submit --class ru.sem.apache_spark_test.InMemorySpark --conf 'spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j2.xml -Dpoi.csv=*path_to_places_of_interest.csv* -Dpl.csv=*persona_locations.csv* -Dres.csv=*final result file path*' --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j2.xml -Dpoi.csv=*path_to_places_of_interest.csv* -Dpl.csv=*persona_locations.csv* -Dres.csv=*final result file path*' ./spark-places-recommender-jar-with-dependencies.jar```

## Built With

* Java 1.8 - for backend
* Spark - for calculations

## Authors

* **Evgenii Suharev** - *Initial work* - [SAmser93](https://github.com/SAmser93)

## License

This project is licensed under the MIT License - see the LICENSE.md file for details