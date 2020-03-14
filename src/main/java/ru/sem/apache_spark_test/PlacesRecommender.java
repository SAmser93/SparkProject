package ru.sem.apache_spark_test;

import ru.sem.apache_spark_test.objects.Persona;
import ru.sem.apache_spark_test.objects.PlaceOfInterest;

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
    public static List<PlaceOfInterest> recommendPopularPlaces(Persona p){
//        1. Присылаем данные о пользователе, смотрим, где он живёт.
        ArrayList<PlaceOfInterest> res = new ArrayList<>();
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
