package ru.sem.apache_spark_test.objects;

import org.apache.commons.csv.CSVRecord;

//import java.time.LocalDate;

public class PlaceOfInterest {

    public static final String[] HEADERS = { "Place_id", "Name", "Category", "Description", "Latitude", "Longitude", "Area_id", "Date"};

/*• Идентификатор места
  •	Название
  •	Категория
  •	Описание
  •	Широта (latitude)
  •	Долгота (longitude)
  •	Идентификатор области
  •	Дата (ггггммдд) (Первый день месяца, за который есть данные)*/

    private int id;
    private String name;
    private String category;
    private String description;
    private double latitude;
    private double longitude;
    private int areaId;
//    LocalDate date; //TODO привести
    private String date;

    public PlaceOfInterest(int id, String name, String category, String description, double latitude, double longitude, int areaId, String date) {
        this.id = id;
        this.name = name;
        this.category = category;
        this.description = description;
        this.latitude = latitude;
        this.longitude = longitude;
        this.areaId = areaId;
        this.date = date;
    }

    public PlaceOfInterest(String[] csvLine) {
        this.id = Integer.parseInt(csvLine[0]);
        this.name = csvLine[1];
        this.category = csvLine[2];
        this.description = csvLine[3];
        this.latitude = Double.parseDouble(csvLine[4]);
        this.longitude = Double.parseDouble(csvLine[5]);
        this.areaId = Integer.parseInt(csvLine[6]);
        this.date = csvLine[7];
    }

    public PlaceOfInterest(CSVRecord record) {
        this.id = Integer.parseInt(record.get(HEADERS[0]));
        this.name = record.get(HEADERS[1]);
        this.category = record.get(HEADERS[2]);
        this.description = record.get(HEADERS[3]);
        this.latitude = Double.parseDouble(record.get(HEADERS[4]));
        this.longitude = Double.parseDouble(record.get(HEADERS[5]));
        this.areaId = Integer.parseInt(record.get(HEADERS[6]));
        this.date = record.get(HEADERS[7]);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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
