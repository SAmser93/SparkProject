package ru.sem.apache_spark_test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.sem.apache_spark_test.dao.DataSource;
import ru.sem.apache_spark_test.objects.Persona;
import ru.sem.apache_spark_test.objects.PersonaLocation;
import ru.sem.apache_spark_test.objects.PlaceOfInterest;
import ru.sem.apache_spark_test.spark.SparkQueries;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.List;

public class InMemorySpark {

    private static Logger logger = LogManager.getLogger(DataSource.class);

    private static final String POI_CSV_FILE_PATH = System.getProperty("poi.csv","src/main/resources/places_of_interest.csv");
    private static final String PERS_LOC_CSV_FILE_PATH = System.getProperty("pl.csv","src/main/resources/persona_locations.csv");

    //"In-memory db"
    private static Dataset<Row> POIdf;
    private static Dataset<Row> PLdf;

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
                //TODO: конфиги
                .option("mode", "DROPMALFORMED")
                .schema(PlaceOfInterest.SCHEMA)
                .csv(POI_CSV_FILE_PATH);

        POIdf.createOrReplaceTempView("place_of_interest");
        POIdf.printSchema();

        PLdf = spark.read()
                //TODO: конфиги
                .option("mode", "DROPMALFORMED")
                .schema(PersonaLocation.SCHEMA)
                .csv(PERS_LOC_CSV_FILE_PATH);

        PLdf.createOrReplaceTempView("persona_locations");
        PLdf.printSchema();

        /*
            TODO:
            Пример. Можно объединить персоны в группы, выявить для каждой группы множество мест. Далее предложить персонам из одной группы места для этой группы отсортированные тем или иным способом.
            1. Запрос на ИД персоны. По нему берём область и ищем по ней PLки за определённый срез (пока последний день/последняя неделя)
            2. Сортируем по популярности и выводим. Рекомендация места у меня будет - процент тех, кто в месте побывал
            3. Предусмотреть, что объем данных может превышать несколько терабайт - ???
         */

        POIdf.show();
        SparkQueries.printPLDF(PLdf, 5);

        logger.info("PLdf size = {}", PLdf.count());

        Persona p = new Persona(1, "Vova");

        //1 Запрос на ИД персоны. По нему берём область
        Dataset<Row> PersonAreaDF = SparkQueries.getLastKnownPersonaLocation(PLdf, p.getId());
        PersonAreaDF.show();
        int area_id = (int)PersonAreaDF.collectAsList().get(0).get(0);
        logger.info("area_id = {}", area_id);

        //ищем по ней PLки за определённый срез (пока последний день/последняя неделя)
        LocalDateTime to = LocalDateTime.of(2019, Month.FEBRUARY, 28, 0, 0);
        LocalDateTime from = to.minusMonths(1);

        Dataset<Row> AreaPersLocDF = SparkQueries.getPersonaLocationsByAreaDate(PLdf, area_id, from, to);
        SparkQueries.printPLDF(AreaPersLocDF, 5);

        logger.info("PLdf size after filter = {}", AreaPersLocDF.count());

        //2. Сортируем по популярности и выводим. Рекомендация места у меня будет - процент тех, кто в месте побывал
        /*
            - По корычам найти место
            - Добавить его в общую мапу мест
            - Просуммировать кол-во по этим местам
            - Отсортировать
            - Вывести какое-то кол-во, посчитать процент
         */

        List<Row> plList = AreaPersLocDF.collectAsList();

        System.out.println(plList);

//        SparkQueries.getPoiByCoordinates(AreaPersLocDF.limit(1));


    }

}
