package lab2;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class SparkTest {
    final String testTemp1 = "20.04.2022 15:26:35.620,area0,sensor200_temp, 10";
    final String testTemp2 = "20.04.2022 15:30:35.620,area0,sensor200_temp, 12";
    final String testPres1 = "20.04.2022 15:26:35.620,area0,sensor200_pres, 765";
    final String testPres2 = "20.04.2022 15:30:35.620,area0,sensor200_pres, 767";
    final String testHum1 = "20.04.2022 15:26:35.620,area0,sensor200_hum, 20";
    final String testHum2 = "20.04.2022 15:30:35.620,area0,sensor200_hum, 22";

    final String testTemp3 = "20.04.2022 16:26:35.620,area0,sensor200_temp, 10";
    final String testTemp4 = "20.04.2022 16:30:35.620,area0,sensor200_temp, 12";
    final String testPres3 = "20.04.2022 16:26:35.620,area0,sensor200_pres, 765";
    final String testPres4 = "20.04.2022 16:30:35.620,area0,sensor200_pres, 767";
    final String testHum3 = "20.04.2022 16:26:35.620,area0,sensor200_hum, 20";
    final String testHum4 = "20.04.2022 16:30:35.620,area0,sensor200_hum, 22";



    SparkSession ss = new SparkSession.Builder().
            master("local").
            appName("SparkApp").
            getOrCreate();
    /**
     * Test if the function rounds the time properly while keeping the original log structure
     **/
    @Test
    public void testPrepareEvents(){
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> testRdd = sc.parallelize(Arrays.asList(testHum1));
        JavaRDD<String> result = EventCounter.prepareEvents(testRdd);
        List<String> res = result.collect();
        assert res.iterator().next().equals("20.04.2022 15:00:00.000,area0,sensor200_hum, 20");
    }

    /**
     * Test if the function counts mean over time and area properly. All logs were produced in
     *the same hour.
     * */
    @Test
    public void testGetTemps(){
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> testRdd = sc.parallelize(Arrays.asList(testTemp1, testTemp2));
        JavaRDD<String> lines = EventCounter.prepareEvents(testRdd);
        JavaRDD<Log> result = EventCounter.getTemps(lines);
        List<Log> res = result.collect();
        Log out = res.iterator().next();
        assert out.getKey().equals("20.04.2022 15:00:00.000,area0");
        assert out.getValue() == 11;
    }

    /**
     * Test if the function counts mean over time and area properly. All logs were produced in
     * the same hour.
     * */
    @Test
    public void testGetPress(){
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> testRdd = sc.parallelize(Arrays.asList(testPres1, testPres2));
        JavaRDD<String> lines = EventCounter.prepareEvents(testRdd);
        JavaRDD<Log> result = EventCounter.getPress(lines);
        List<Log> res = result.collect();
        Log out = res.iterator().next();
        assert out.getKey().equals("20.04.2022 15:00:00.000,area0");
        assert out.getValue() == 766;
    }

    /**
     * Test if the function counts mean over time and area properly. All logs were produced in
     * the same hour.
     * */
    @Test
    public void testGetHum(){
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> testRdd = sc.parallelize(Arrays.asList(testHum1, testHum2));
        JavaRDD<String> lines = EventCounter.prepareEvents(testRdd);
        JavaRDD<Log> result = EventCounter.getHum(lines);
        List<Log> res = result.collect();
        Log out = res.iterator().next();
        assert out.getKey().equals("20.04.2022 15:00:00.000,area0");
        assert out.getValue() == 21;
    }

    /**
     * Tests the whole pipeline. Logs for the same area for two different hours.
     * */
    @Test
    public void globalTest(){
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> testRdd = sc.parallelize(Arrays.asList(testTemp1,
                testTemp2, testTemp3, testTemp4,testHum1, testHum2,
                testHum3, testHum4, testPres1, testPres2, testPres3, testPres4));
        JavaRDD<String> lines = EventCounter.prepareEvents(testRdd);
        JavaRDD<Log> temp = EventCounter.getTemps(lines);
        List<Log> tempLogs = temp.collect();
        Iterator<Log> it = tempLogs.iterator();
        Log tempOut = it.next();
        assert tempOut.getKey().equals("20.04.2022 15:00:00.000,area0");
        assert tempOut.getValue() == 11;
        tempOut = it.next();
        assert tempOut.getKey().equals("20.04.2022 16:00:00.000,area0");
        assert tempOut.getValue() == 11;

        JavaRDD<Log> press = EventCounter.getPress(lines);
        List<Log> pressLogs = press.collect();
        it = pressLogs.iterator();
        Log pressOut = it.next();
        assert pressOut.getKey().equals("20.04.2022 15:00:00.000,area0");
        assert pressOut.getValue() == 766;
        pressOut = it.next();
        assert pressOut.getKey().equals("20.04.2022 16:00:00.000,area0");
        assert pressOut.getValue() == 766;

        JavaRDD<Log> hum = EventCounter.getHum(lines);
        List<Log> humLogs = hum.collect();
        it = humLogs.iterator();
        Log humOut = it.next();
        assert humOut.getKey().equals("20.04.2022 15:00:00.000,area0");
        assert humOut.getValue() == 21;
        humOut = it.next();
        assert humOut.getKey().equals("20.04.2022 16:00:00.000,area0");
        assert humOut.getValue() == 21;

    }




}
