package ru.sem.apache_spark_test.objects;

import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.sql.types.StructType;

import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class PersonaLocation extends AbstractSparkObject {

    public static final String[] HEADERS = { COLUMNS.Persona_id.name(), COLUMNS.Date_time.name(), COLUMNS.Latitude.name(),
            COLUMNS.Longitude.name(), COLUMNS.Area_id.name(), COLUMNS.Date.name()
    };
    public static final StructType SCHEMA = new StructType()
            .add(HEADERS[0], "int")
            .add(HEADERS[1], "string")
            .add(HEADERS[2], "double")
            .add(HEADERS[3], "double")
            .add(HEADERS[4], "int")
            .add(HEADERS[5], "string");
    private int persona_id;

/*
    •	Идентификатор персоны
    •	Время (ггггммдд_чч) (Время с точностью до часа, где прибывала персона)
    •	Широта (latitude) (Часть координаты пребывания персоны в течении часа. Усредненная)
    •	Долгота (longitude) (Часть координаты пребывания персоны в течении часа. Усредненная)
    •	Идентификатор области
    •	Дата (ггггммдд) (Первый день месяца, за который есть данные)
*/
    private LocalDateTime date_time;
    private double latitude;
    private double longitude;
    private int area_id;
    private LocalDate date;
    public PersonaLocation() {}

    public PersonaLocation(int persona_id, LocalDateTime date_time, double latitude, double longitude, int area_id, LocalDate date) {
        this.persona_id = persona_id;
        this.date_time = date_time;
        this.latitude = latitude;
        this.longitude = longitude;
        this.area_id = area_id;
        this.date = date;
    }

    public PersonaLocation(CSVRecord record) {
        this.persona_id = Integer.parseInt(record.get(HEADERS[0]));
        this.date_time = LocalDateTime.parse(record.get(HEADERS[1]), Date_time_formatter);
        this.latitude = Double.parseDouble(record.get(HEADERS[2]));
        this.longitude = Double.parseDouble(record.get(HEADERS[3]));
        this.area_id = Integer.parseInt(record.get(HEADERS[4]));
        this.date = LocalDate.parse(record.get(HEADERS[5]), Date_formatter);
    }

    public PersonaLocation parseFromResultSet(ResultSet rsSelect) {
        try{
            this.persona_id = rsSelect.getInt(HEADERS[0]);
            this.date_time = LocalDateTime.parse(rsSelect.getString(HEADERS[1]), Date_time_formatter);
            this.latitude = rsSelect.getDouble(HEADERS[2]);
            this.longitude = rsSelect.getDouble(HEADERS[3]);
            this.area_id = rsSelect.getInt(HEADERS[4]);
            this.date = LocalDate.parse(rsSelect.getString(HEADERS[5]), Date_formatter);
        } catch (Exception z){
            z.printStackTrace();
            return null;
        }
        return this;
    }

    public int getPersona_id() {
        return persona_id;
    }

    public void setPersona_id(int persona_id) {
        this.persona_id = persona_id;
    }

    public LocalDateTime getDate_time() {
        return date_time;
    }

    public void setDate_time(LocalDateTime date_time) {
        this.date_time = date_time;
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

    public int getArea_id() {
        return area_id;
    }

    public void setArea_id(int area_id) {
        this.area_id = area_id;
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    @Override
    public void insertToCSV(CSVPrinter printer){
        try {
            printer.printRecord(
                    this.persona_id,
                    this.date_time.format(Date_time_formatter),
                    this.latitude,
                    this.longitude,
                    this.area_id,
                    this.date.format(Date_formatter)
            );
        } catch (Exception z) {
            z.printStackTrace();
        }
    }

    public enum COLUMNS {
        Persona_id,
        Date_time,
        Latitude,
        Longitude,
        Area_id,
        Date
    }
}
