package ru.sem.apache_spark_test.dao;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.sem.apache_spark_test.objects.PersonaLocation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PlacesRecommenderDAO {

    private static Logger logger = LogManager.getLogger(PlacesRecommenderDAO.class);

    private final static String CLEAR_LOCATIONS_SQL = "delete from persona_locations";

    private final static String INSERT_LOCATION_SQL =
            "insert into persona_locations (persona_id, Date_time, latitude, longitude, Area_id, Date) " +
            "values (?, ?, ?, ?, ?, ?)";

    private final static String SELECT_LAST_KNOWN_LOCATION =
            "select area_id from persona_locations where persona_id = ? and date_time in " +
                    "(select max(date_time) from persona_locations where persona_id = ?) limit 1 ";
    private final static String SELECT_PERSONA_LOCATIONS_FROM_AREA =
            "select * from persona_locations where area_id = ?";
    private final static String SELECT_PERSONA_LOCATIONS_FROM_AREA_EXCEPT_CURRENT_PERSONA =
            "select * from persona_locations where area_id = ? and persona_id != ?";
    private final static String GET_POI_BY_NEARBY_COORDINATES =
            "select * from places_of_interest where latitude between ? and ? and longitude between ? and ?";

    public static void clearLocations() {

        logger.info("clearing persona_locations");

        try(final Connection connection = DataSource.getConnection();
            final PreparedStatement pstm = connection.prepareStatement(CLEAR_LOCATIONS_SQL)){
            pstm.executeUpdate();
        } catch (Exception z) {
            z.printStackTrace();
        }
    }

    public static boolean insertLocation(PersonaLocation pl) {

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

    /**
     * Получить данные о тем, где сейчас персона
     * @param personaId - идентификатор персоны
     * @return - area_id
     */
    public static int getLastKnownPersonaLocation(int personaId) {
        logger.info("getting last area_id for persona -> {}", personaId);

        int res = -1;

        try(final Connection connection = DataSource.getConnection();
            final PreparedStatement pstm = connection.prepareStatement(SELECT_LAST_KNOWN_LOCATION)){
            pstm.setInt(1, personaId);
            try (final ResultSet rsSelect = pstm.executeQuery())
            {
                while(rsSelect.next())
                {
                    res = rsSelect.getInt("area_id");
                }
            }
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }

        return res;
    }

    /**
     * Получить данные о посещениях в области
     * @param areaId - идентификатор области
     * @return список посещений
     */
    public static List<PersonaLocation> getPersonaLocationsFromArea(int areaId) {
        logger.info("getting persona_locations with area_id -> {}", areaId);
        List<PersonaLocation> res = new ArrayList<>();

        try(final Connection connection = DataSource.getConnection();
            final PreparedStatement pstm = connection.prepareStatement(SELECT_PERSONA_LOCATIONS_FROM_AREA_EXCEPT_CURRENT_PERSONA)){
            pstm.setInt(1, areaId);
            try (final ResultSet rsSelect = pstm.executeQuery())
            {
                while(rsSelect.next())
                {
                    PersonaLocation pl = new PersonaLocation();
                    if(pl.parseFromResultSet(rsSelect) != null){
                        res.add(pl);
                    }
                }
            }
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }

        return res;

    }

    /**
     * Получить данные о посещениях в области, без учёта той персоны, для которой эта статистика отбирается
     * @param areaId - идентификатор области
     * @param personaId - идентификатор персоны
     * @return список посещений
     */
    public static List<PersonaLocation> getPersonaLocationsFromArea(int areaId, int personaId) {
        logger.info("getting persona_locations with area_id -> {} and personaId -> {}", areaId, personaId);
        List<PersonaLocation> res = new ArrayList<>();

        try(final Connection connection = DataSource.getConnection();
            final PreparedStatement pstm = connection.prepareStatement(SELECT_PERSONA_LOCATIONS_FROM_AREA)){
            pstm.setInt(1, areaId);
            pstm.setInt(2, personaId);
            try (final ResultSet rsSelect = pstm.executeQuery())
            {
                while(rsSelect.next())
                {
                    PersonaLocation pl = new PersonaLocation();
                    if(pl.parseFromResultSet(rsSelect) != null){
                        res.add(pl);
                    }
                }
            }
        }
        catch(SQLException e)
        {
            throw new RuntimeException(e);
        }

        return res;
    }

}
