package com.spark_example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spark_example.model.Employee;
import com.spark_example.model.EmployeeDetail;
import com.spark_example.model.StateInfo;
import com.spark_example.util.SharedUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ivy.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassTag$;

/**
 * Hello world!
 *
 */

public class App
{
    private static Logger logger = LoggerFactory.getLogger(App.class);

    private static ObjectMapper mapper = new ObjectMapper();
    public static void main( String[] args )
    {
        SparkSession spark = getSparkSession();

        broadcastExample(spark);

        //job11(spark);

        logger.error("spark job compelted");
        stopJob(spark);

    }

    private static void broadcastExample(SparkSession spark) {
        Dataset<StateInfo> stateInfoDataset = SharedUtils.readJsonFile(
            spark,"/Users/munjal-upadhyay/Downloads/pincode.txt", StateInfo.class);

        Dataset<Employee> employeeDataset = SharedUtils.readJsonFile(
            spark,"/Users/munjal-upadhyay/Downloads/word.txt", Employee.class);

        List<StateInfo> stateInfoList=stateInfoDataset.collectAsList();
        Map<Integer,StateInfo> stateInfoMap= new HashMap<>();
        stateInfoList.forEach(ele -> {
            stateInfoMap.putIfAbsent(ele.getPincode(),ele);
        });

        Broadcast<Map<Integer, StateInfo>> stateInfoMapBV = spark.sparkContext()
            .broadcast(stateInfoMap, ClassTag$.MODULE$.apply(Map.class));

        Dataset<EmployeeDetail> employeeDetailDataset = employeeDataset.mapPartitions(
            (MapPartitionsFunction<Employee,EmployeeDetail>)iterator -> {

                logger.error("this code is executing under worker node");
                List<EmployeeDetail> lis=new ArrayList<>();
                Map<Integer, StateInfo>  stateInfoMapLocal  = stateInfoMapBV.getValue(); // this code is executing under worker node

            iterator.forEachRemaining( emp -> {
                EmployeeDetail employeeDetail=new EmployeeDetail();
                employeeDetail.setId(emp.getId());
                employeeDetail.setName(emp.getName());
                employeeDetail.setSurname(emp.getSurname());
                employeeDetail.setSalary(emp.getSalary());
                employeeDetail.setPin(emp.getPin());
                if (stateInfoMapLocal.containsKey(emp.getPin())){
                    StateInfo stateInfo=stateInfoMapLocal.get(emp.getPin());
                    employeeDetail.setCityname(stateInfo.getCityname());
                    employeeDetail.setShortform(stateInfo.getShortform());
                }
                lis.add(employeeDetail);
            });
            return lis.iterator();
        },Encoders.bean(EmployeeDetail.class));

        employeeDetailDataset.foreach((ForeachFunction<EmployeeDetail>)str -> logger.error(" detailed emp {} ",str));
    }

    private static void job11(SparkSession spark) {

        Dataset<String> stringDS = readTextFile(spark);
        stringDS.cache(); // it is always better to cache whn you read input from text file or db.
        printEachElementOfRDD(stringDS);
        logger.error("this is driver logs : going ahead... --> ");


        Dataset<Employee> employeeDS = mapPartitionExample(stringDS);
        employeeDS.cache();

        logger.error( " number of partitions are {} ", employeeDS.javaRDD().getNumPartitions());
        //employeeDS.glom().collect().forEach(ele ->logger.error(" glome out"+ele ));

        logger.error("this is driver logs : going ahead 1 ... --> ");

        employeeDS.foreach((ForeachFunction<Employee>)str -> logger.error("printing value of {} ",str));

        logger.error("count {}",employeeDS.count());
    }

    @Nullable
    private static Dataset<Employee> mapPartitionExample(Dataset<String> stringDS) {

        Dataset<Employee> employeeJavaRDD = stringDS.mapPartitions(
            (MapPartitionsFunction<String, Employee>) stringIterator -> {
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
                try {
                    Employee fromJson = mapper.readValue(str,
                        Employee.class);
                    logger.error("created objects for "+fromJson);
                    empList.add(fromJson);
                } catch (Exception e) {
                    logger.error("Exception while converting to teamDetails due to {}",
                        StringUtils.getStackTrace(e));
                }
            });

            logger.error("mapPartitions count {} ",empList.size());

            return empList.iterator(); // note : here we are returning iterator.
        },Encoders.bean(Employee.class));
        return employeeJavaRDD;
    }


    private static void printEachElementOfRDD(Dataset<String> stringJRDD) {
        stringJRDD.foreach((ForeachFunction<String>)str -> logger.error("printing value of {} ",str));
    }

    private static Dataset<String> readTextFile(SparkSession spark) {
        String filePath = "/Users/munjal-upadhyay/Downloads/word.txt";

        //you can load dataset like below , this is how we load s3 files also.
        Dataset<String> stringDS = spark.read().textFile(filePath).repartition(3);
        return stringDS;
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
