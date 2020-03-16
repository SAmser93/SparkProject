package ru.sem.apache_spark_test.objects;

import org.apache.commons.csv.CSVRecord;
import org.apache.spark.sql.types.StructType;

//import java.time.LocalDate;

public class PlaceOfInterest {

    public static final String[] HEADERS = { "Place_id", "Name", "Category", "Description", "Latitude", "Longitude", "Area_id", "Date"};
    public static final StructType SCHEMA = new StructType()
            .add(HEADERS[0], "int")
            .add(HEADERS[1], "string")
            .add(HEADERS[2], "string")
            .add(HEADERS[3], "string")
            .add(HEADERS[4], "double")
            .add(HEADERS[5], "double")
            .add(HEADERS[6], "int")
            .add(HEADERS[7], "string");

/*• Идентификатор места
  •	Название
  •	Категория
  •	Описание
  •	Широта (latitude)
  •	Долгота (longitude)
  •	Идентификатор области
  •	Дата (ггггммдд) (Первый день месяца, за который есть данные)*/

    private int place_id;
    private String name;
    private String category;
    private String description;
    private double latitude;
    private double longitude;
    private int area_id;
//    LocalDate date; //TODO привести
    private String date;

    public PlaceOfInterest(int Place_id, String name, String category, String description, double latitude, double longitude, int area_id, String date) {
        this.place_id = Place_id;
        this.name = name;
        this.category = category;
        this.description = description;
        this.latitude = latitude;
        this.longitude = longitude;
        this.area_id = area_id;
        this.date = date;
    }

    public PlaceOfInterest(String[] csvLine) {
        this.place_id = Integer.parseInt(csvLine[0]);
        this.name = csvLine[1];
        this.category = csvLine[2];
        this.description = csvLine[3];
        this.latitude = Double.parseDouble(csvLine[4]);
        this.longitude = Double.parseDouble(csvLine[5]);
        this.area_id = Integer.parseInt(csvLine[6]);
        this.date = csvLine[7];
    }

    public PlaceOfInterest(CSVRecord record) {
        this.place_id = Integer.parseInt(record.get(HEADERS[0]));
        this.name = record.get(HEADERS[1]);
        this.category = record.get(HEADERS[2]);
        this.description = record.get(HEADERS[3]);
        this.latitude = Double.parseDouble(record.get(HEADERS[4]));
        this.longitude = Double.parseDouble(record.get(HEADERS[5]));
        this.area_id = Integer.parseInt(record.get(HEADERS[6]));
        this.date = record.get(HEADERS[7]);
    }

    public int getPlace_id() {
        return place_id;
    }

    public void setPlace_id(int place_id) {
        this.place_id = place_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
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

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }


}
