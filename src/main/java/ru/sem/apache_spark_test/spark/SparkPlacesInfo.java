package ru.sem.apache_spark_test.spark;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sem.apache_spark_test.InMemorySpark;
import ru.sem.apache_spark_test.objects.FinalResult;
import ru.sem.apache_spark_test.objects.Persona;
import ru.sem.apache_spark_test.objects.PersonaLocation;
import ru.sem.apache_spark_test.objects.PlaceOfInterest;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class SparkPlacesInfo {

    private static Logger logger = LogManager.getLogger(InMemorySpark.class);

    /**
     * Получение данных о посещениях для конкретного пользователя за срез времени
     * @param p - объект персоны
     * @param from - дата начала среза
     * @param to - дата конца среза
     * @return - список объектов FinalResult
     */
    public static List<FinalResult> getInfoAboutLocations(Persona p, LocalDateTime from, LocalDateTime to){

        //POI только за выбранную дату. В планах - и без дубликатов
        Dataset<Row> filteredPOIs = SparkQueries.filerPOIByDate(InMemorySpark.POIdf, from, to);
//        filteredPOIs.select("*").show();
        //1 Запрос на ИД персоны. По нему берём область
        Dataset<Row> PersonAreaDF = SparkQueries.getLastKnownPersonaLocation(InMemorySpark.PLdf, p.getId());
        PersonAreaDF.show();
        int area_id = (int)PersonAreaDF.collectAsList().get(0).get(0);
        logger.info("area_id = {}", area_id);

        //ищем по ней PL за определённый срез (в данном случае - за месяц)
        Dataset<Row> AreaPersLocDF = SparkQueries.getPersonaLocationsByAreaDate(InMemorySpark.PLdf, area_id, from, to);
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

        //TODO: Убрать дубликаты, не пользуясь банвальным отбрасыванием значений POI
        //Мапа вида "Ид_достопримечательности -> кол-во посещений
        Map<Integer, Integer> poiVisitsCount = new HashMap<>();
        int visits_total = 0;

        List<Row> plList = AreaPersLocDF.select(PersonaLocation.COLUMNS.Longitude.name(), PersonaLocation.COLUMNS.Latitude.name()).collectAsList();
        System.out.println(plList);
        for(Row r : plList) {
            PlaceOfInterest temp = SparkQueries.getPoiByCoordinates(filteredPOIs,
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

        List<FinalResult> res = new ArrayList<>();

        //Формирование финального результата в формате csv
        for(Map.Entry<Integer, Integer> e: topTen.entrySet()){
            PlaceOfInterest poi = new PlaceOfInterest(
                    filteredPOIs.select("*")
                            .filter(
                                    filteredPOIs.col(PlaceOfInterest.COLUMNS.Place_id.name())
                                            .equalTo(e.getKey())
                            )
                            .limit(1)
                            .collectAsList()
                            .get(0)
            );
            FinalResult temp = new FinalResult(p.getId(), e.getKey(), ((double) e.getValue())/ visits_total,
                    poi.getName(), poi.getDescription(), poi.getLatitude(), poi.getLongitude(), poi.getArea_id(), poi.getDate());
            res.add(temp);
        }

        return res;
    }
}
