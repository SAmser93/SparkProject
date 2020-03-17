package ru.sem.apache_spark_test.objects;

import org.apache.commons.csv.CSVPrinter;
import org.apache.spark.sql.types.StructType;

import java.text.NumberFormat;
import java.util.Locale;

public class FinalResult {

    public static final String[] HEADERS = { COLUMNS.Persona_id.name(), COLUMNS.Place_id.name(), COLUMNS.Recommendation_ratio.name(),
            COLUMNS.Name.name(), COLUMNS.Description.name(),  COLUMNS.Latitude.name(), COLUMNS.Longitude.name(),
            COLUMNS.Area_id.name(), COLUMNS.Date.name()
    };
    public static final StructType SCHEMA = new StructType()
            .add(HEADERS[0], "string")
            .add(HEADERS[1], "int")
            .add(HEADERS[2], "double")
            .add(HEADERS[3], "string")
            .add(HEADERS[4], "string")
            .add(HEADERS[5], "double")
            .add(HEADERS[6], "double")
            .add(HEADERS[7], "int")
            .add(HEADERS[8], "string");

    /*
        •	Идентификатор персоны
        •	Идентификатор места
        •	Рекомендация места *
        •	Название места
        •	Описание места
        •	Широта
        •	Долгота
        •	Идентификатор области
        •	Дата
     */
    private int persona_id;
    private int place_id;
    private double recommendation_ratio;
    private String name;
    private String description;
    private double latitude;
    private double longitude;
    private int areaId;
    private String date;

    public FinalResult(int persona_id, int place_id, double recommendation_ratio, String name, String description, double latitude, double longitude, int areaId, String date) {
        this.persona_id = persona_id;
        this.place_id = place_id;
        this.recommendation_ratio = recommendation_ratio;
        this.name = name;
        this.description = description;
        this.latitude = latitude;
        this.longitude = longitude;
        this.areaId = areaId;
        this.date = date;
    }

    public int getPersona_id() {
        return persona_id;
    }

    public void setPersona_id(int persona_id) {
        this.persona_id = persona_id;
    }

    public int getPlace_id() {
        return place_id;
    }

    public void setPlace_id(int place_id) {
        this.place_id = place_id;
    }

    public double getRecommendation_ratio() {
        return recommendation_ratio;
    }

    public void setRecommendation_ratio(double recommendation_ratio) {
        this.recommendation_ratio = recommendation_ratio;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public void insertToCSV(CSVPrinter printer){
        try {
            printer.printRecord(
                    this.persona_id,
                    this.place_id,
                    NumberFormat.getPercentInstance(Locale.US).format(this.recommendation_ratio),
                    this.name,
                    this.description,
                    this.latitude,
                    this.longitude,
                    this.areaId,
                    this.date
            );
        } catch (Exception z) {
            z.printStackTrace();
        }
    }

    public enum COLUMNS {
        Persona_id,
        Place_id,
        Recommendation_ratio,
        Name,
        Description,
        Latitude,
        Longitude,
        Area_id,
        Date
    }
}
