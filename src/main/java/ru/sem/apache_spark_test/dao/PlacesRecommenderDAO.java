package ru.sem.apache_spark_test.dao;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.sem.apache_spark_test.objects.PersonaLocation;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class PlacesRecommenderDAO {

    private static Logger logger = LogManager.getLogger(PlacesRecommenderDAO.class);

    private final static String CLEAR_LOCATIONS_SQL = "delete from persona_locations";
    private final static String INSERT_LOCATION_SQL = "insert into persona_locations (persona_id, Date_time, latitude, longitude, Area_id, Date) " +
            "values (?, ?, ?,?, ?, ?)";

    public static void clearLocations() {

        logger.info("clearing persona_locations");

        try(final Connection connection = DataSource.getConnection();
            final PreparedStatement pstm = connection.prepareStatement(CLEAR_LOCATIONS_SQL)){
            pstm.executeUpdate();
        } catch (Exception z) {
            z.printStackTrace();
        }
    }

    public static boolean insertLocation(PersonaLocation pl){

        logger.info("insert persona_locations fro p.id -> {}", pl.getPersonaId());

        try(final Connection connection = DataSource.getConnection();
            final PreparedStatement pstm = connection.prepareStatement(INSERT_LOCATION_SQL)){
            //TODO: batch update
            int i = 0;
            pstm.setInt(++i, pl.getPersonaId());
            pstm.setString(++i, pl.getDateTime());
            pstm.setDouble(++i, pl.getLatitude());
            pstm.setDouble(++i, pl.getLongitude());
            pstm.setInt(++i, pl.getAreaId());
            pstm.setString(++i, pl.getDate());
            pstm.executeUpdate();
        } catch (Exception z) {
            z.printStackTrace();
            return false;
        }
        return true;
    }

}
