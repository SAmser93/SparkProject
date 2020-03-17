import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class Main_local {

    public static void main( String[] args ) {

        final JavaSparkContext sc = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Spark user-activity")
                        .setMaster("local[2]")            //local - означает запуск в локальном режиме.
                        .set("spark.driver.host", "localhost")    //это тоже необходимо для локального режима
        );

        //Здесь могла быть загрузка из файла sc.textFile("users-visits.log");
        //Но я решил применить к входным данным метод parallelize(); Для наглядности

        List<String> visitsLog = Arrays.asList(
                "user_id:0000, habrahabr.ru",
                "user_id:0001, habrahabr.ru",
                "user_id:0002, habrahabr.ru",
                "user_id:0000, abc.ru",
                "user_id:0000, yxz.ru",
                "user_id:0002, qwe.ru",
                "user_id:0002, zxc.ru",
                "user_id:0001, qwe.ru",
                "user_id:0000, qwe.ru"
                //итд, дофантазируйте дальше сами :)
        );

        JavaRDD<String> visits = sc.parallelize(visitsLog);

        //из каждой записи делаем пары: ключ (user_id), значение (1 - как факт посещения)
        // (user_id:0000 : 1)
        JavaPairRDD<String, Integer> pairs = visits.mapToPair(
                (String s) -> {
                    String[] kv = s.split(",");
                    return new Tuple2<>(kv[0], 1);
                }
        );

        //суммируем факты посещений для каждого user_id
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(
//                (Integer a, Integer b) -> a + b
                Integer::sum
        );

        //сиртируем по Value и возвращаем первые 10 запсисей
        List<Tuple2<String, Integer>> top10 = counts.takeOrdered(
                10,
                new CountComparator()
        );

        System.out.println(top10);


        //        JavaRDD<PlaceOfInterest> rdd_records = sc.textFile("places_of_interest.csv").map(
//                (Function<String, PlaceOfInterest>) line -> {
//                    // Here you can use JSON
//                    // Gson gson = new Gson();
//                    // gson.fromJson(line, Record.class);
//                    String[] fields = line.split(",");
//                    PlaceOfInterest sd = new PlaceOfInterest(
//                            Integer.parseInt(fields[0]),
//                            fields[1].trim(),
//                            fields[2].trim(),
//                            fields[3],
//                            Double.parseDouble(fields[4]),
//                            Double.parseDouble(fields[5]),
//                            Integer.parseInt(fields[6]),
//                            fields[7]
//                            );
//                    return sd;
//                });
//
//        System.out.println(rdd_records);


    }

    public static class CountComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {

            return o2._2()-o1._2();
        }
    }
}
