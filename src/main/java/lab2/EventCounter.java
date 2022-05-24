package lab2;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;



@AllArgsConstructor
@Slf4j
public class EventCounter {

    /**
     * Takes a string as an input, floors timestamp to an hour.
     * Return string of the same format but with rounded time
     * */
    private static String roundTime(String line){
        String[] line_arr = line.split(",");
        String time = line_arr[0].split(":")[0];
        String area = line_arr[1];
        String res = time + ":00:00.000," + area+"," + line_arr[2]+","+line_arr[3];
        return res;
    }

    /**
     * Returns first two fields of the line (time, area)
     * */
    private static String extract_key(String line){
        String[] lineArr = line.split(",");
        String key = lineArr[0] + "," +lineArr[1];
        return key;
    }

    /**
     * Calculates the mean value of temperature among records with the same timestamp and area
     * */
    public static JavaRDD<Log> getTemps(JavaRDD<String> lines){
        JavaRDD<String> temp = lines.filter(s -> s.split(",")[2].split("_")[1].equals("temp"));
        JavaPairRDD<String, Tuple2<Integer, Integer>> values = temp.mapToPair(s -> new Tuple2<>(extract_key(s),
                new Tuple2<>(Integer.parseInt(s.split(",")[3].replace(" ", "")),1 )));
        JavaPairRDD<String, Tuple2<Integer, Integer>> total = values.reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2()+b._2()));
        JavaPairRDD<String, Integer> avg = total.mapValues(v -> v._1() / v._2());
        JavaRDD<Log> avg_temp = avg.map(line -> {
            Log log = new Log();
            log.setKey(line._1());
            log.setValue(line._2());
            return log;
        });
        return avg_temp;
    }

    /**
     * Calculates the mean value of pressure among records with the same timestamp and area
     * */
    public static JavaRDD<Log> getPress(JavaRDD<String> lines){
        JavaRDD<String> temp = lines.filter(s -> s.split(",")[2].split("_")[1].equals("pres"));
        JavaPairRDD<String, Tuple2<Integer, Integer>> values = temp.mapToPair(s -> new Tuple2<>(extract_key(s),
                new Tuple2<>(Integer.parseInt(s.split(",")[3].replace(" ", "")),1 )));
        JavaPairRDD<String, Tuple2<Integer, Integer>> total = values.reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2()+b._2()));
        JavaPairRDD<String, Integer> avg = total.mapValues(v -> v._1() / v._2());
        JavaRDD<Log> avg_press = avg.map(line -> {
            Log log = new Log();
            log.setKey(line._1());
            log.setValue(line._2());
            return log;
        });
        return avg_press;
    }

    /**
     * Calculates the mean value of humidity among records with the same timestamp and area
     * */
    public static JavaRDD<Log> getHum(JavaRDD<String> lines){
        JavaRDD<String> temp = lines.filter(s -> s.split(",")[2].split("_")[1].equals("hum"));
        JavaPairRDD<String, Tuple2<Integer, Integer>> values = temp.mapToPair(s -> new Tuple2<>(extract_key(s),
                new Tuple2<>(Integer.parseInt(s.split(",")[3].replace(" ", "")),1 )));
        JavaPairRDD<String, Tuple2<Integer, Integer>> total = values.reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2()+b._2()));
        JavaPairRDD<String, Integer> avg = total.mapValues(v -> v._1() / v._2());
        JavaRDD<Log> avg_hum = avg.map(line -> {
            Log log = new Log();
            log.setKey(line._1());
            log.setValue(line._2());
            return log;
        });
        return avg_hum;
    }

    /**
     * Creates an RDD containing logs with rounded timestamps
     * */
    public static JavaRDD<String> prepareEvents(JavaRDD<String> lines ){
        JavaRDD<String> rounded_lines = lines.map(s -> roundTime(s));
        return rounded_lines;
    }

}