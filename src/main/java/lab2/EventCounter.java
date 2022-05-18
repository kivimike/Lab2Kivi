package lab2;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.functions;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Map;
import java.util.regex.Pattern;

import static java.time.temporal.ChronoField.YEAR;


@AllArgsConstructor
@Slf4j
public class EventCounter {
    private static final Pattern SPACE = Pattern.compile(" ");
    // Формат времени логов - н-р, 'Oct 26 13:54:06'
    private static DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendPattern("MMM dd HH:mm:ss")
            .parseDefaulting(YEAR, 2018)
            .toFormatter();

    /**
     * Функция подсчета количества логов разного уровня в час.
     * Парсит строку лога, в т.ч. уровень логирования и час, в который событие было зафиксировано.
     * //    * @param inputDataset - входной DataSet для анализа
     * @return результат подсчета в формате JavaRDD
     */
//    public static JavaRDD<Row> countLogLevelPerHour(Dataset<String> inputDataset) {
//        Dataset<String> words = inputDataset.map(s -> Arrays.toString(s.split("\n")), Encoders.STRING());
//
//        Dataset<LogLevelHour> logLevelHourDataset = words.map(s -> {
//                    String[] logFields = s.split(",");
//                    LocalDateTime date = LocalDateTime.parse(logFields[2], formatter);
//                    return new LogLevelHour(logFields[1], date.getHour());
//                }, Encoders.bean(LogLevelHour.class))
//                .coalesce(1);
//
//        // Группирует по значениям часа и уровня логирования
//        Dataset<Row> t = logLevelHourDataset.groupBy("hour", "logLevel")
//                .count()
//                .toDF("hour", "logLevel", "count")
//                // сортируем по времени лога - для красоты
//                .sort(functions.asc("hour"));
//        log.info("===========RESULT=========== ");
//        t.show();
//        return t.toJavaRDD();
//    }

    private static String roundTime(String line){
        String[] line_arr = line.split(",");
        String time = line_arr[0].split(":")[0];
        String area = line_arr[1];
        String res = time + ":00:00.000," + area+"," + line_arr[2]+","+line_arr[3];
        return res;
    }

    private static String extract_key(String line){
        String[] lineArr = line.split(",");
        String key = lineArr[0] + "," +lineArr[1];
        return key;
    }

    public static JavaPairRDD<String, Integer> getTemps(JavaRDD<String> lines){
        JavaRDD<String> temp = lines.filter(s -> s.split(",")[2].split("_")[1].equals("temp"));
        JavaPairRDD<String, Tuple2<Integer, Integer>> values = temp.mapToPair(s -> new Tuple2<>(extract_key(s),
                new Tuple2<>(Integer.parseInt(s.split(",")[3].replace(" ", "")),1 )));
        JavaPairRDD<String, Tuple2<Integer, Integer>> total = values.reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2()+b._2()));
        JavaPairRDD<String, Integer> avg = total.mapValues(v -> v._1() / v._2());
        return avg;
    }

    public static JavaPairRDD<String, Integer> getPress(JavaRDD<String> lines){
        JavaRDD<String> temp = lines.filter(s -> s.split(",")[2].split("_")[1].equals("pres"));
        JavaPairRDD<String, Tuple2<Integer, Integer>> values = temp.mapToPair(s -> new Tuple2<>(extract_key(s),
                new Tuple2<>(Integer.parseInt(s.split(",")[3].replace(" ", "")),1 )));
        JavaPairRDD<String, Tuple2<Integer, Integer>> total = values.reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2()+b._2()));
        JavaPairRDD<String, Integer> avg = total.mapValues(v -> v._1() / v._2());
        return avg;
    }

    public static JavaPairRDD<String, Integer> getHum(JavaRDD<String> lines){
        JavaRDD<String> temp = lines.filter(s -> s.split(",")[2].split("_")[1].equals("hum"));
        JavaPairRDD<String, Tuple2<Integer, Integer>> values = temp.mapToPair(s -> new Tuple2<>(extract_key(s),
                new Tuple2<>(Integer.parseInt(s.split(",")[3].replace(" ", "")),1 )));
        JavaPairRDD<String, Tuple2<Integer, Integer>> total = values.reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2()+b._2()));
        JavaPairRDD<String, Integer> avg = total.mapValues(v -> v._1() / v._2());
        return avg;
    }

    public static JavaRDD<String> prepareEvents(JavaRDD<String> lines ){
        JavaRDD<String> rounded_lines = lines.map(s -> roundTime(s));
        return rounded_lines;
    }

}