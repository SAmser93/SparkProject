package ru.sem.apache_spark_test;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import ru.sem.apache_spark_test.objects.Persona;
import ru.sem.apache_spark_test.objects.PersonaLocation;
import ru.sem.apache_spark_test.objects.PlaceOfInterest;
import scala.Tuple2;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class Task {

    private static final String POI_CSV_FILE_PATH = "src/main/resources/places_of_interest.csv";
    private static final String PERS_LOC_CSV_FILE_PATH = "src/main/resources/persona_locations.csv";

    //TODO: Вынести в общий метод как-то
    public static ArrayList<PlaceOfInterest> readListPOIFromCSV(String path) {

        ArrayList<PlaceOfInterest> places = new ArrayList<>();

        try {
            Reader in = new FileReader(path);
            Iterable<CSVRecord> records = CSVFormat.DEFAULT
                    .withHeader(Arrays.toString(PlaceOfInterest.HEADERS))
                    .withFirstRecordAsHeader()
                    .parse(in);
            for (CSVRecord record : records) {
                PlaceOfInterest obj = new PlaceOfInterest(record);
                places.add(obj);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return places;
    }

    public static ArrayList<PersonaLocation> readListPLFromCSV(String path) {

        ArrayList<PersonaLocation> places = new ArrayList<>();

        try {
            Reader in = new FileReader(path);
            Iterable<CSVRecord> records = CSVFormat.DEFAULT
                    .withHeader(Arrays.toString(PersonaLocation.HEADERS))
                    .withFirstRecordAsHeader()
                    .parse(in);
            for (CSVRecord record : records) {
                PersonaLocation obj = new PersonaLocation(record);
                places.add(obj);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return places;
    }

    /*
        Метод "Предложить персоне 3 популярных места".
        Идея:
        1. Присылаем данные о пользователе, смотрим, где он живёт.
        2. Отбираем пользователей, где он живёт, считаем их посещения в виде мапы "Ид_места, посещения"
        3. Сортируем по популярности и выводим
     */
    public List<PlaceOfInterest> recommendPopularPlaces(Persona p){
        ArrayList<PlaceOfInterest> res = new ArrayList<>();
        return res;
    }

    /*
        Метод "Предложить персоне 3 места, что понравятся ему".
        Идея:
        1. Присылаем данные о пользователе, смотрим, в каких местах он (предположительно) был.
        2. Отбираем пользователей, которые были в тех же местах, в которых был он, считаем их посещения в виде мапы "Ид_места, посещения"
            * Это лобовой вариант. Скорее всего, нужно будет ещё добавить условие "Учитывать персон, которые были в как минимум 3х тех же местах, что и ты"
        3. Сортируем по популярности и выводим
     */
    public List<PlaceOfInterest> recommendPlacesForPerson(Persona p){
        ArrayList<PlaceOfInterest> res = new ArrayList<>();
        return res;
    }

    public static void main(String[] args) {
        final JavaSparkContext sc = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Spark user-activity")
                        .setMaster("local[2]")            //local - означает запуск в локальном режиме.
                        .set("spark.driver.host", "localhost")    //это тоже необходимо для локального режима
        );

//        Выходные данные
        //•	Идентификатор персоны
        //•	Идентификатор места
        //•	Рекомендация места *
        //•	Название места
        //•	Описание места
        //•	Широта
        //•	Долгота
        //•	Идентификатор области
        //•	Дата
//        Рекомендация места * – это оценка того на сколько данные место подходит к персоне. Необходимо предложить и реализовать свой вариант расчета оценки.
//        Пример. Можно объединить персоны в группы, выявить для каждой группы множество мест. Далее предложить персонам из одной группы места для этой группы отсортированные тем или иным способом.

        /*
            1. Map людей по областям. Причём ещё надо, похоже, по месту ещё определять, был ли пользователь в какй-то из известных нам точек.
                Всё это джойнить в некий общий объект, полагаю. Причём ещё надо по дате проверять.
                Делать мапу "Персона - пары посещений объектов". Из этого потом можно считать популярность
            2. Лобовой вариант - показывать данные в виде "3 места на персону, отсортированные по кол-ву посещений"
            3. Менее лобовой - матрица смежности с теми местами, куда ходили другие люди с этого объекта
         */

        //2 Сначала всё таки с местами
        ArrayList<PlaceOfInterest> places = readListPOIFromCSV(POI_CSV_FILE_PATH);
        JavaRDD<PlaceOfInterest> placesRDD = sc.parallelize(places);

        ArrayList<PersonaLocation> locations = readListPLFromCSV(PERS_LOC_CSV_FILE_PATH);
        JavaRDD<PersonaLocation> locRDD = sc.parallelize(locations);

        //из каждой записи делаем пары: ключ (Area_id), значение (1 - как факт наличия в области)
        JavaPairRDD<Integer, Integer> pairs = locRDD.mapToPair(
                (PersonaLocation poi) -> new Tuple2<>(poi.getAreaId(), 1)
        );

        JavaPairRDD<Integer, Integer> counts = pairs.reduceByKey(
//                (Integer a, Integer b) -> a + b
                Integer::sum
        );

        List<Tuple2<Integer, Integer>> top2 = counts.takeOrdered(
                2,
                new CountComparator()
        );

        System.out.println(top2);

        Persona persona = new Persona(1, "Vova");


    }

    //Нужен для корректного сравнения в условиях RDD
    public static class CountComparator implements Comparator<Tuple2<Integer, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
            return o2._2()-o1._2();
        }
    }

}
