package ru.sem.apache_spark_test.objects;

public enum Category {

    //В идеале, брать из БД

    THEATER("Театр"),
    SHOP("Магазин"),
    SIGHT("Достопримечательность"),
    PARK("Парк");

    String name;

    Category(String name) {
        this.name = name;
    }

    public String getName() { return name; }
}
