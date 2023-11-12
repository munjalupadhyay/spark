package com.spark_example;

import com.spark_example.model.Employee;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.ivy.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.api.Symbols.SymbolApi;

/**
 * Hello world!
 *
 */

public class App
{
    private static Logger logger = LoggerFactory.getLogger(App.class);
    public static void main( String[] args )
    {
        SparkSession spark = getSparkSession();
        job11(spark);

        logger.error("spark job compelted");
        stopJob(spark);

    }

    private static void job11(SparkSession spark) {

        JavaRDD<String> stringJRDD = readTextFile(spark);
        printEachElementOfRDD(stringJRDD);
        logger.error("this is driver logs : going ahead... --> ");

        JavaRDD<Employee> employeeJavaRDD = mapPartitionExample(stringJRDD);
        employeeJavaRDD.cache();

        logger.error( " number of partitions are {} ", employeeJavaRDD.getNumPartitions());
        //employeeJavaRDD.glom().collect().forEach(ele ->logger.error(" glome out"+ele ));

        logger.error("this is driver logs : going ahead 1 ... --> ");

        printEachElementOfRDD(employeeJavaRDD);

        logger.error("count {}",employeeJavaRDD.count());
    }

    @Nullable
    private static JavaRDD<Employee> mapPartitionExample(JavaRDD<String> stringJRDD) {

        JavaRDD<Employee> employeeJavaRDD = stringJRDD.mapPartitions(stringIterator -> {
            List<Employee> empList = new ArrayList<>();
            // below line is a myth, you can not create a DB connection and share across all worker nodes ,
            // commone sense is you can not share a connection across multiple nodes just by passiong the object as parametr.
            // what you should do is : the db connection should initialize only per partition. and not for each record in the partition.
            // that can be achieved by above line.
            logger.error("mapPartitions initialization : this should be called only once");

            System.out.println("this should be called only once per partition");

            if (stringIterator==null){
                return null;
            }
           /*

           example using hasNext() and next()

            while(stringIterator.hasNext()){
                String str=stringIterator.next();
                logger.error("creating objects for "+str);
                String[] fields = str.split(",");
                Long id = Long.parseLong(fields[0]);
                String name = String.valueOf(fields[1]);
                String surname = String.valueOf(fields[2]);
                Employee emp = new Employee(id, name, surname);
                logger.error("cereated object {}", emp);
                empList.add(emp);
            }

            */

            // Below is example using forEachRemaining() (better version of using hasNext() and next().)

            stringIterator.forEachRemaining(str -> {
                logger.error("creating objects for "+str);
                String[] fields = str.split(",");
                Long id = Long.parseLong(fields[0]);
                String name = String.valueOf(fields[1]);
                String surname = String.valueOf(fields[2]);
                Employee emp = new Employee(id, name, surname);
                logger.error("cereated object {}", emp);
                empList.add(emp);
            });

            logger.error("mapPartitions count {} ",empList.size());

            return empList.iterator(); // note : here we are returning iterator.
        });
        return employeeJavaRDD;
    }

    private static void printEachElementOfRDD(JavaRDD<?> stringJRDD) {
        stringJRDD.foreach(str -> logger.error("printing value of {} ",String.valueOf(str)));
    }

    private static JavaRDD<String> readTextFile(SparkSession spark) {
        JavaRDD<String> stringJRDD = spark.sparkContext()
            .textFile("/Users/munjal-upadhyay/Downloads/word.txt", 3)
            .toJavaRDD();
        return stringJRDD;
    }


    private static SparkSession getSparkSession() {
        SparkConf conf = new SparkConf(true);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        return spark;
    }

    public static void stopJob(SparkSession spark) {
        logger.info("stopping spark called...");
        try {
            Thread.sleep(6000);
            logger.info("m=stopJob; delay finised, going to stop");
        } catch (Exception e) {
            logger.error("m=stopJob; exception in delay; Exception={}", StringUtils.getStackTrace(e));
        }
        logger.info("stopping spark app");
        spark.stop();
        logger.info("stopping spark driver");
    }
}
