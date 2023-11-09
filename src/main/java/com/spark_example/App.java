package com.spark_example;

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

        SparkSession spark = SparkSession.builder().getOrCreate();

        Dataset<Row>  dataset= spark.read().csv("/Users/munjal-upadhyay/Downloads/word.txt");

        dataset.foreachPartition(iteration -> {
            while (iteration.hasNext()){
                String str=iteration.next().get(0).toString();
                logger.error(str);
                System.out.println(str);
            }
        });
        System.out.println( "Hello World!" );

    }
}
