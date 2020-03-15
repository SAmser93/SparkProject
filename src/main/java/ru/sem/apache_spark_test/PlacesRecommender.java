package ru.sem.apache_spark_test;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import ru.sem.apache_spark_test.dao.PlacesRecommenderDAO;
import ru.sem.apache_spark_test.objects.Persona;
import ru.sem.apache_spark_test.objects.PersonaLocation;
import ru.sem.apache_spark_test.objects.PlaceOfInterest;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PlacesRecommender {

    /**
     Метод "Предложить персоне 3 популярных места".
     Идея:
     1. Присылаем данные о пользователе, смотрим, где он живёт.
     2. Отбираем пользователей, где он живёт, считаем их посещения в виде мапы "Ид_места, посещения"
     3. Сортируем по популярности и выводим
     */
    public static List<PlaceOfInterest> recommendPopularPlaces(JavaSparkContext sc, int personaId){
//        1. Присылаем данные о пользователе, смотрим, где он живёт.
        ArrayList<PlaceOfInterest> res = new ArrayList<>();
        int currentArea = PlacesRecommenderDAO.getLastKnownPersonaLocation(personaId);
//        2. Отбираем пользователей, где он живёт, считаем их посещения в виде мапы "Ид_места, посещения"
        List<PersonaLocation> areaLocations = PlacesRecommenderDAO.getPersonaLocationsFromArea(currentArea, personaId);
//        3. Сортируем по популярности и выводим
        JavaRDD<PersonaLocation> locRDD = sc.parallelize(areaLocations);
//        JavaPairRDD<Integer, Integer> visitsPairs = locRDD.mapToPair(
//                (PersonaLocation pl) -> {
//                    new Tuple2<>(pl.getPoiID(), 1);
//                }
//        );
        return res;
    }

    /**
     Метод "Предложить персоне 3 места, что понравятся ей".
     Идея:
     1. Присылаем данные о пользователе, смотрим, в каких местах он (предположительно) был.
     2. Отбираем пользователей, которые были в тех же местах, в которых был он, считаем их посещения в виде мапы "Ид_места, посещения"
     * Это лобовой вариант. Скорее всего, нужно будет ещё добавить условие "Учитывать персон, которые были в как минимум 3х тех же местах, что и ты"
     3. Сортируем по популярности и выводим
     */
    public static List<PlaceOfInterest> recommendPlacesForPerson(Persona p){
        ArrayList<PlaceOfInterest> res = new ArrayList<>();
        return res;
    }
}
