spark :

start spark locally :

goto path : /opt/homebrew/Cellar/apache-spark/3.5.0/libexec/sbin
run ./start-master.sh
run ./start-worker.sh spark://munjal-upadhyay.local:7077 (this url you can get form http://localhost:8080/jobs/ )

spark jobs : http://localhost:8080/
spars web UI : http://localhost:4040/
spark history server : http://localhost:18080/


learning resources :
https://www.youtube.com/watch?v=WCO_r_lgrJU&list=PLLa_h7BriLH0FzTY5aBFpH-vciOiEf4Br&index=12
https://mageswaran1989.medium.com/apache-spark-interview-questions-f28d92ddf09a

command to submit job to spark :
spark-submit --class com.spark_example.App --master spark://munjal-upadhyay.local:7077 --deploy-mode cluster --num-executors 1 ./spark_example-0.0.1-SNAPSHOT-jar-with-dependencies.jar

spark-submit --class com.spark_example.App --master spark://munjal-upadhyay.local:7077 --deploy-mode client --num-executors 1 ./spark_example-0.0.1-SNAPSHOT-jar-with-dependencies.jar

spark-submit --class com.spark_example.App --master spark://munjal-upadhyay.local:7077 --deploy-mode cluster --num-executors 1 ./spark_example-0.0.1-SNAPSHOT-jar-with-dependencies.jar


application logs will be under : SPARK_HOME/work directory
in my case it is : /opt/homebrew/Cellar/apache-spark/3.5.0/libexec/work/driver-20231106183303-0003

difference between cache() and persist() : https://sparkbyexamples.com/spark/spark-difference-between-cache-and-persist/


Type of operation in spark :
  Action : this will not return rdd. count(),collect(),top(),forEach()
  Transformation : this will return rdd. all transformation are lazy.
                   so if you do not call action then no operation will be executed.

Type of transformation :
  Narrow transformation : data do not cross partition. map(), filter()
  Wide transformation : data will cross the partition. groupbyKey() and reducebyKey()

By default rdd will not be be cached in memory :
  for example :

    textFileRdd = sc.textFile("/user/emp.txt")
    textFileRdd.count();
    textFileRdd.count();

    above code will load the file twice in the memory, because you have not cached the loaded data.
    and you have to be careful if you apply multiple action operation . because  each action operation will do all the work again.
    in below example the text file will be loaded twice , first time for counting and second time for getting top element.

    for below code , don't be under impression that you are doing operations on same the rdd so it will not be loaded again.
    textFileRdd = sc.textFile("/user/emp.txt")
    textFileRdd.count();
    textFileRdd.top();

    read more : https://stackoverflow.com/questions/28981359/why-do-we-need-to-call-cache-or-persist-on-a-rdd


when to use cache and when not to use :
  it is possible that node memory might get full and in that case all you cached rdd will start evicting.
  so this might reduce your performance because your cached rdd will be recompted again.

  read more : https://www.unraveldata.com/resources/to-cache-or-not-to-cache/

reason to use mapPartition over map() :
  read below url. there are very important obseration.
  read more : https://dzone.com/articles/mappartitions-in-apache-spark-5-key-benefits
              https://ajaygupta-spark.medium.com/map-vs-mappartitions-in-apache-spark-765247634640#:~:text=Meaning%20the%20processing%20function%20provided,to%20single%20input%20partition%2C%20together.

you can convert your rdd to dataset :
  https://stackoverflow.com/questions/45326796/convert-rdd-to-dataset-in-java-spark

RDD vs. FataFrame vs. DataSet : usuallly it is better to use dataset.

  RDD : basic objects
  Dataframe : created on top of RDD. here you work wil unstructured data (of type Row).
  Dataset : created on top of Dataframe. here you work with structured data (of your created object type)


  read more :
    https://www.databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html
    https://data-flair.training/blogs/apache-spark-rdd-vs-dataframe-vs-dataset/
    https://sparkbyexamples.com/spark/spark-rdd-vs-dataframe-vs-dataset/



