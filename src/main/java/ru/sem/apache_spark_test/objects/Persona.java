package ru.sem.apache_spark_test.objects;

public class Persona {
    private int id;
    private String name;

    public Persona(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() { return id; }

    public String getName() { return name; }
}
