package ru.sem.apache_spark_test.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sem.apache_spark_test.objects.PersonaLocation;
import ru.sem.apache_spark_test.objects.PlaceOfInterest;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class SparkQueries {

    private static final DateTimeFormatter Date_formatter = DateTimeFormatter.ofPattern("YYYYMMDD");
    private static final DateTimeFormatter Date_time_formatter = DateTimeFormatter.ofPattern("YYYYMMDD"+"_HH");

    public static void printPLDF(Dataset<Row> ds, int limit) {
        ds.select(PersonaLocation.COLUMNS.Persona_id.name(),
                PersonaLocation.COLUMNS.Date_time.name(),
                PersonaLocation.COLUMNS.Area_id.name()
        ).limit(limit).show();
    }

    public static Dataset<Row> getLastKnownPersonaLocation (Dataset<Row> ds, int persona_id) {
        return ds.select(PersonaLocation.COLUMNS.Area_id.name())
                .filter(
                        ds.col(PersonaLocation.COLUMNS.Persona_id.name())
                                .equalTo(persona_id)
                )
                .orderBy(
                        ds.col(PersonaLocation.COLUMNS.Date_time.name())
                                .desc()
                )
                .limit(1);
    }

    public static Dataset<Row> getPersonaLocationsByAreaDate(Dataset<Row> ds, int area_id, LocalDateTime from, LocalDateTime to) {
        return ds.select(
                PersonaLocation.COLUMNS.Persona_id.name(),
                PersonaLocation.COLUMNS.Date_time.name(),
                PersonaLocation.COLUMNS.Latitude.name(),
                PersonaLocation.COLUMNS.Longitude.name(),
                PersonaLocation.COLUMNS.Area_id.name(),
                PersonaLocation.COLUMNS.Date.name()
        ).filter(
                ds.col(PersonaLocation.COLUMNS.Area_id.name())
                        .equalTo(area_id)
        ).filter(
                ds.col(PersonaLocation.COLUMNS.Date_time.name())
                        .gt(from.format(Date_time_formatter))
        ).filter(
                ds.col(PersonaLocation.COLUMNS.Date_time.name())
                        .lt(to.format(Date_time_formatter))
        );
    }

    public static PlaceOfInterest getPoiByCoordinates(Dataset<Row> ds, double latitude, double longitude) {
        Dataset<Row> poi = ds.select(
                ds.col("*")
        ).filter(
                ds.col(PlaceOfInterest.COLUMNS.Latitude.name())
                .gt(latitude-0.1)
        ).filter(
                ds.col(PlaceOfInterest.COLUMNS.Latitude.name())
                        .lt(latitude+0.1)
        ).filter(
                ds.col(PlaceOfInterest.COLUMNS.Longitude.name())
                        .gt(longitude-0.1)
        ).filter(
                ds.col(PlaceOfInterest.COLUMNS.Longitude.name())
                        .lt(longitude+0.1)
        ).orderBy(
                ds.col(PlaceOfInterest.COLUMNS.Date.name())
                        .desc()
        ).limit(1);

//        poi.select(PlaceOfInterest.COLUMNS.Place_id.name()).show();
        List<Row> r = poi.select(poi.col("*")).collectAsList();
        if(r.size() > 0){
            try{
                return new PlaceOfInterest(r.get(0));
            } catch (Exception z) {
                z.printStackTrace();
            }
        }
        return null;
    }
}
