package com.spark_example.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spark_example.App;
import java.util.ArrayList;
import java.util.List;
import org.apache.ivy.util.StringUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SharedUtils {

  private static Logger logger = LoggerFactory.getLogger(SharedUtils.class);

  private static ObjectMapper mapper = new ObjectMapper();


  public static <T> Dataset<T> readJsonFile(SparkSession spark,String filePath,Class<T> valueType) {

    // TODO : read SparkSession object form application context , rather than passing in method args.

    Dataset<String> stringDS = spark.read().textFile(filePath).repartition(3);

    Dataset<T> resultJavaRDD = stringDS.map(
        (MapFunction<String, T>) str -> {

          logger.error("creating objects for "+str);
            try {
              T fromJson = mapper.readValue(str, valueType);
              return fromJson;
            } catch (Exception e) {
              logger.error("Exception while converting to teamDetails due to {}",
                  StringUtils.getStackTrace(e));
              throw e;
            }
        }, Encoders.bean(valueType));

    return resultJavaRDD;
  }

}
