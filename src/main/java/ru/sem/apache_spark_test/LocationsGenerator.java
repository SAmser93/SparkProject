package ru.sem.apache_spark_test;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import ru.sem.apache_spark_test.objects.PersonaLocation;
import ru.sem.apache_spark_test.objects.PlaceOfInterest;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public class LocationsGenerator {

    /**
     * Генерация описаний местоположения
     * @param args
     */
    public static void main(String[] args) {

        //Для указания кол-ва случайнео генерируемых персон
        final int minPersons = Integer.parseInt(System.getProperty("min.persons","7"));
        final int maxPersons = Integer.parseInt(System.getProperty("max.persons","15"));
        //Для указания кол-ва случайнео генерируемых мест на одну персону
        final int minPlaces = Integer.parseInt(System.getProperty("min.places","2"));
        final int maxPlaces = Integer.parseInt(System.getProperty("max.places","5"));

        String poiCSVFilePath = System.getProperty("poi.csv", "src/main/resources/places_of_interest.csv");
        String pers_locCSVFilePath = System.getProperty("pl.csv", "src/main/resources/persona_locations.csv");

        ArrayList<PlaceOfInterest> places = new ArrayList<>();

        try {

            Reader in = new FileReader(poiCSVFilePath);
            Iterable<CSVRecord> records = CSVFormat.DEFAULT
                    .withHeader(Arrays.toString(PlaceOfInterest.HEADERS))
                    .withFirstRecordAsHeader()
                    .parse(in);
            for (CSVRecord record : records) {
                PlaceOfInterest poi = new PlaceOfInterest(record);
                places.add(poi);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

//        PlacesRecommenderDAO.clearLocations();

        try {
            FileWriter out = new FileWriter(pers_locCSVFilePath);
            CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT
                    .withHeader(Arrays.toString(PersonaLocation.HEADERS))
                    .withFirstRecordAsHeader());
            printer.printRecord(PersonaLocation.HEADERS);
            int personsNum = ThreadLocalRandom.current().nextInt(minPersons, maxPersons);
            System.out.println("personsNum = " + personsNum);
            for(int i = 0; i < personsNum; i++) {
                int placesNum = ThreadLocalRandom.current().nextInt(minPlaces, maxPlaces);
                for(int j = 0; j < placesNum; j++) {
                    PlaceOfInterest randomPOI = places.get(ThreadLocalRandom.current().nextInt(places.size()-1));
                    PersonaLocation tempPl = new PersonaLocation(
                            i+1,
                            randomPOI.getDate().atTime(ThreadLocalRandom.current().nextInt(0, 23), 0, 0),
                            //окрестности выбранного poi
                            ThreadLocalRandom.current().nextDouble(randomPOI.getLatitude()-0.1f, randomPOI.getLatitude())+0.1f,
                            ThreadLocalRandom.current().nextDouble(randomPOI.getLongitude()-0.1f, randomPOI.getLongitude()+0.1f),
                            randomPOI.getArea_id(),
                            randomPOI.getDate()
                            );
                    tempPl.insertToCSV(printer);
//                    PlacesRecommenderDAO.insertLocation(tempPl);
                }
            }
            out.flush();
            out.close();
        } catch (Exception z) {
            z.printStackTrace();
        }
    }
}
