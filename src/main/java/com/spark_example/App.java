package com.spark_example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        Dataset<Row>  dataset= spark.read().csv("/Users/munjal-upadhyay/Downloads/word.txt");

        dataset.foreachPartition(iteration -> {
            while (iteration.hasNext()){
                String str=iteration.next().get(0).toString();
                logger.error(str);
                System.out.println(str);
            }
        });
    }


    private static SparkSession getSparkSession() {
        SparkConf conf = new SparkConf(true);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        return spark;
    }

    public static void stopJob(SparkSession spark) {
        logger.info("stopping spark called...");
        logger.info("stopping spark app");
        spark.stop();
        logger.info("stopping spark driver");
    }
}
