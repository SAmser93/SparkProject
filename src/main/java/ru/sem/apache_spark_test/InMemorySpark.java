package ru.sem.apache_spark_test;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.sem.apache_spark_test.objects.FinalResult;
import ru.sem.apache_spark_test.objects.Persona;
import ru.sem.apache_spark_test.objects.PersonaLocation;
import ru.sem.apache_spark_test.objects.PlaceOfInterest;
import ru.sem.apache_spark_test.spark.SparkPlacesInfo;
import ru.sem.apache_spark_test.spark.SparkQueries;

import java.io.FileWriter;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.*;

import static java.time.temporal.TemporalAdjusters.lastDayOfMonth;

public class InMemorySpark {

    private static Logger logger = LogManager.getLogger(InMemorySpark.class);

    private static final String POI_CSV_FILE_PATH = System.getProperty("poi.csv", "src/main/resources/places_of_interest.csv");
    private static final String PERS_LOC_CSV_FILE_PATH = System.getProperty("pl.csv", "src/main/resources/persona_locations.csv");
    private static final String FINAL_RES_CSV_FILE_PATH = System.getProperty("res.csv", "src/main/resources/final_result.csv");
    //"In-memory db", которую сделаем доступной везде
    public static Dataset<Row> POIdf;
    public static Dataset<Row> PLdf;

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Places recommendation example")
                .master("local[2]") //TODO: [2]?
                .getOrCreate();

        //TODO: поискать настройки, мб для этого и нужно ограничение
        spark.conf().set("spark.driver.memory", "2g");
        spark.conf().set("spark.driver.maxResultSize", "6g");
        spark.conf().set("spark.executor.memory", "4g");
        spark.conf().set("spark.driver.host", "localhost");

        POIdf = spark.read()
                .option("mode", "DROPMALFORMED")
                .schema(PlaceOfInterest.SCHEMA)
                .csv(POI_CSV_FILE_PATH);
                // подобным образом можно убрать дубликаты, но вообще надо бы превратить посещения подобных точек в посещения одний из них, а не просто отбросить
//                .dropDuplicates(PlaceOfInterest.COLUMNS.Latitude.name(), PlaceOfInterest.COLUMNS.Longitude.name(), PlaceOfInterest.COLUMNS.Date.name());

        POIdf.createOrReplaceTempView("place_of_interest");
        POIdf.printSchema();

        PLdf = spark.read()
                .option("mode", "DROPMALFORMED")
                .schema(PersonaLocation.SCHEMA)
                .csv(PERS_LOC_CSV_FILE_PATH);

        PLdf.createOrReplaceTempView("persona_locations");
        PLdf.printSchema();

        /*
            Общий алгоритм:
            Пример. Можно объединить персоны в группы, выявить для каждой группы множество мест. Далее предложить персонам из одной группы места для этой группы отсортированные тем или иным способом.
            1. Запрос на ИД персоны. По нему берём область и ищем по ней PLки за определённый срез (пока последний день/последняя неделя)
            2. Сортируем по популярности и выводим. Рекомендация места у меня будет - процент тех, кто в месте побывал
         */

        POIdf.show();
        SparkQueries.printPLDF(PLdf, 5);

        logger.info("PLdf size = {}", PLdf.count());

        Persona p = new Persona(1, "Vova");
        LocalDateTime from = LocalDate.of(2019, Month.FEBRUARY, 1).atStartOfDay();
        LocalDateTime to = from.with(lastDayOfMonth());

        List<FinalResult> finalResults = SparkPlacesInfo.getInfoAboutLocations(p, from, to);

        logger.info("finalResults -> {}", finalResults);

        try {
            FileWriter out = new FileWriter(FINAL_RES_CSV_FILE_PATH);
            CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT
                    .withHeader(Arrays.toString(FinalResult.HEADERS))
                    .withFirstRecordAsHeader());

            printer.printRecord(FinalResult.HEADERS);
            for(FinalResult f : finalResults){
                f.insertToCSV(printer);
            }
            out.flush();
            out.close();
        } catch (Exception z) {
            z.printStackTrace();
        }

    }

}
