package lab2;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.functions;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static java.time.temporal.ChronoField.YEAR;

@Slf4j
public class SparkApplication {

    /**
     * @param args - args[0]: входной файл, args[1] - выходная папка
     */
//    public static void main2(String[] args) {
//        if (args.length < 2) {
//            throw new RuntimeException("Usage: java -jar SparkSQLApplication.jar input.file outputDirectory");
//        }
//
//        log.info("Appliction started!");
//        log.debug("Application started");
//        SparkSession sc = SparkSession
//                .builder()
//                .master("local")
//                .appName("SparkSQLApplication")
//                .getOrCreate();
//
//        Dataset<String> df = sc.read().text(args[0]).as(Encoders.STRING());
//        log.info("===============COUNTING...================");
//        JavaRDD<Row> result = EventCounter.countLogLevelPerHour(df);
//        log.info("============SAVING FILE TO " + args[1] + " directory============");
//        result.saveAsTextFile(args[1]);
//    }

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: pack.WordCount <file>");
            System.exit(1);
        }
        log.info("Application started!");
        log.debug("Application started");

        SparkConf sparkConf = new SparkConf().setAppName("lab2.SparkApplication");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0], 1);
        log.info("===============COUNTING...================");
        JavaRDD<String> events = EventCounter.prepareEvents(lines);
        JavaPairRDD<String, Integer> temps = EventCounter.getTemps(events);
        JavaPairRDD<String, Integer> press = EventCounter.getPress(events);
        JavaPairRDD<String, Integer> hum = EventCounter.getHum(events);
        temps.repartition(1).saveAsTextFile("temps");
        press.repartition(1).saveAsTextFile("press");
        hum.repartition(1).saveAsTextFile("hum");
        //List<Tuple2<String, Integer>> output = result.collect();
//        for (Tuple2 tuple : output) {
//            System.out.println(tuple._1() + ": " + tuple._2());
//        }
        ctx.stop();
    }
}