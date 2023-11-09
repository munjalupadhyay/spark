spark :

start spark locally :

goto path : /opt/homebrew/Cellar/apache-spark/3.5.0/libexec/sbin
run ./start-master.sh
run ./start-worker.sh spark://munjal-upadhyay.local:7077 (this url you can get form http://localhost:8080/jobs/ )


learning resources :
https://www.youtube.com/watch?v=WCO_r_lgrJU&list=PLLa_h7BriLH0FzTY5aBFpH-vciOiEf4Br&index=12
https://mageswaran1989.medium.com/apache-spark-interview-questions-f28d92ddf09a

command to submit job to spark :
spark-submit --class com.spark_example.App --master spark://munjal-upadhyay.local:7077 --deploy-mode cluster --num-executors 1 ./spark_example-0.0.1-SNAPSHOT-jar-with-dependencies.jar

spark-submit --class com.spark_example.App --master spark://munjal-upadhyay.local:7077 --deploy-mode client --num-executors 1 ./spark_example-0.0.1-SNAPSHOT-jar-with-dependencies.jar


application logs will be under : SPARK_HOME/work directory
in my case it is : /opt/homebrew/Cellar/apache-spark/3.5.0/libexec/work/driver-20231106183303-0003

difference between cache() and persist() : https://sparkbyexamples.com/spark/spark-difference-between-cache-and-persist/

