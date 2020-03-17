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
import ru.sem.apache_spark_test.spark.SparkQueries;

import java.io.FileWriter;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.*;
import java.util.stream.Collectors;

public class InMemorySpark {

    private static Logger logger = LogManager.getLogger(InMemorySpark.class);

    private static String POI_CSV_FILE_PATH;
    private static String PERS_LOC_CSV_FILE_PATH;
    private static String FINAL_RES_CSV_FILE_PATH;
    //"In-memory db"
    private static Dataset<Row> POIdf;
    private static Dataset<Row> PLdf;

    static {
        try {
            POI_CSV_FILE_PATH = System.getProperty("poi.csv", Paths.get(ClassLoader.getSystemResource("places_of_interest.csv").toURI()).toString());
        } catch (Exception e) {
            logger.error("Error while opening places_of_interest.csv -> {}", e.getMessage());
            System.exit(-1);
        }

        try {
            PERS_LOC_CSV_FILE_PATH = System.getProperty("pl.csv", Paths.get(ClassLoader.getSystemResource("persona_locations.csv").toURI()).toString());
        } catch (Exception e) {
            logger.error("Error while opening persona_locations -> {}, it will be created", e.getMessage());
            PERS_LOC_CSV_FILE_PATH = "src/main/resources/persona_locations.csv";
        }

        try {
            FINAL_RES_CSV_FILE_PATH = System.getProperty("res.csv", Paths.get(ClassLoader.getSystemResource("final_result.csv").toURI()).toString());
        } catch (Exception e) {
            logger.error("Error while opening final_result -> {}, it will be created", e.getMessage());
            FINAL_RES_CSV_FILE_PATH = "src/main/resources/final_result.csv";
        }
    }

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

        //ищем по ней PL за определённый срез (в данном случае - за месяц)
        LocalDateTime to = LocalDateTime.of(2019, Month.FEBRUARY, 28, 0, 0);
        LocalDateTime from = to.minusMonths(1);

        Dataset<Row> AreaPersLocDF = SparkQueries.getPersonaLocationsByAreaDate(PLdf, area_id, from, to);
        //Убираем свои посещения
        AreaPersLocDF = AreaPersLocDF.filter(
                AreaPersLocDF.col(PersonaLocation.COLUMNS.Persona_id.name()).notEqual(p.getId())
        ).select("*");
        SparkQueries.printPLDF(AreaPersLocDF, 5);

        logger.info("PLdf size after filter = {}", AreaPersLocDF.count());

        //2. Сортируем по популярности и выводим. Рекомендация места у меня будет - процент тех, кто в месте побывал
        /*
            - По координатам найти место
            - Добавить его в общую мапу мест
            - Просуммировать кол-во по этим местам
            - Отсортировать
         */

        //TODO: перенести на RDD
        //Мапа вида "Ид_достопримечательности -> кол-во посещений
        Map<Integer, Integer> poiVisitsCount = new HashMap<>();
        int visits_total = 0;

        List<Row> plList = AreaPersLocDF.select(PersonaLocation.COLUMNS.Longitude.name(), PersonaLocation.COLUMNS.Latitude.name()).collectAsList();
        System.out.println(plList);
        for(Row r : plList) {
            PlaceOfInterest temp = SparkQueries.getPoiByCoordinates(POIdf,
                    r.getDouble(r.fieldIndex(PersonaLocation.COLUMNS.Latitude.name())),
                    r.getDouble(r.fieldIndex(PersonaLocation.COLUMNS.Longitude.name()))
            );
            if(temp != null) {
                poiVisitsCount.merge(temp.getPlace_id(), 1, Integer::sum);
                visits_total++;
            }
        }

        logger.info("not sorted map -> {}", poiVisitsCount);

        Map<Integer,Integer> topTen =
                poiVisitsCount.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                        .limit(10)
                        .collect(Collectors.toMap(
                                Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        logger.info("sorted map -> {}", topTen);

        List<FinalResult> finalResults = new ArrayList<>();

        //Формирование финального результата в формате csv
        for(Map.Entry<Integer, Integer> e: topTen.entrySet()){
            PlaceOfInterest poi = new PlaceOfInterest(
                    POIdf.select("*")
                            .filter(
                                    POIdf.col(PlaceOfInterest.COLUMNS.Place_id.name())
                                            .equalTo(e.getKey())
                            )
                            .limit(1)
                            .collectAsList()
                            .get(0)
            );
            FinalResult temp = new FinalResult(p.getId(), e.getKey(), ((double) e.getValue())/ visits_total,
                    poi.getName(), poi.getDescription(), poi.getLatitude(), poi.getLongitude(), poi.getArea_id(), poi.getDate());
            finalResults.add(temp);
        }

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
