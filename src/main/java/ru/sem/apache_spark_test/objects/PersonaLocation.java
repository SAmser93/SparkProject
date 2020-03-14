package ru.sem.apache_spark_test.objects;

import org.apache.commons.csv.CSVRecord;

import java.io.Serializable;

public class PersonaLocation implements Serializable {

    public static final String[] HEADERS = { "Persona_id", "Date_time", "Latitude", "Longitude", "Area_id", "Date"};

/*
    •	Идентификатор персоны
    •	Время (ггггммдд_чч) (Время с точностью до часа, где прибывала персона)
    •	Широта (latitude) (Часть координаты пребывания персоны в течении часа. Усредненная)
    •	Долгота (longitude) (Часть координаты пребывания персоны в течении часа. Усредненная)
    •	Идентификатор области
    •	Дата (ггггммдд) (Первый день месяца, за который есть данные)
*/

    private int personaId;
    private String dateTime;
    private double latitude;
    private double longitude;
    private int areaId;
    private String date;

    public PersonaLocation(int personaId, String dateTime, double latitude, double longitude, int areaId, String date) {
        this.personaId = personaId;
        this.dateTime = dateTime;
        this.latitude = latitude;
        this.longitude = longitude;
        this.areaId = areaId;
        this.date = date;
    }

    public PersonaLocation(CSVRecord record) {
        this.personaId = Integer.parseInt(record.get(HEADERS[0]));
        this.dateTime = record.get(HEADERS[1]);
        this.latitude = Double.parseDouble(record.get(HEADERS[2]));
        this.longitude = Double.parseDouble(record.get(HEADERS[3]));
        this.areaId = Integer.parseInt(record.get(HEADERS[4]));
        this.date = record.get(HEADERS[5]);
    }

    public int getPersonaId() {
        return personaId;
    }

    public void setPersonaId(int personaId) {
        this.personaId = personaId;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public int getAreaId() {
        return areaId;
    }

    public void setAreaId(int areaId) {
        this.areaId = areaId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
