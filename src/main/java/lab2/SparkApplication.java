package lab2;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.*;


import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapColumnTo;


@Slf4j
public class SparkApplication {

    public static void main(String[] args) throws Exception {

        log.info("Application started!");
        log.debug("Application started");

        SparkConf sparkConf = new SparkConf().setAppName("lab2.SparkApplication");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        log.info("=============LOADING DATA...==============");
        JavaRDD<String> lines = CassandraJavaUtil.javaFunctions(ctx)
                .cassandraTable("my_keyspace", "raw_logs", mapColumnTo(String.class)).select("data");
        log.info("===============COUNTING...================");
        JavaRDD<String> events = EventCounter.prepareEvents(lines);
        JavaRDD<Log> temps = EventCounter.getTemps(events);
        JavaRDD<Log> press = EventCounter.getPress(events);
        JavaRDD<Log> hum = EventCounter.getHum(events);

        log.info("===========SAVING TO CASSANDRA============");
        SparkSession spark = SparkSession.builder().getOrCreate();

        spark.createDataFrame(temps, Log.class).toDF()
                .write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "my_keyspace")
                .option("table", "temperature").option("confirm.truncate", "true")
                .mode("OVERWRITE").save();

        spark.createDataFrame(press, Log.class).toDF()
                .write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "my_keyspace")
                .option("table", "pressure").option("confirm.truncate", "true")
                .mode("OVERWRITE").save();

        spark.createDataFrame(hum, Log.class).toDF()
                .write()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "my_keyspace")
                .option("table", "humidity").option("confirm.truncate", "true")
                .mode("OVERWRITE").save();

        spark.stop();
        ctx.stop();
    }
}