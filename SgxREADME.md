
# Run 

## Master

`SPARK_LOCAL_IP=127.0.0.1 /usr/lib/jvm/java-8-openjdk/jre/bin/java -cp /home/florian/GIT/sgx-spark/conf/:/home/florian/GIT/sgx-spark/assembly/target/scala-2.11/jars/\* -Xmx1g org.apache.spark.deploy.master.Master --host 127.0.0.1 --port 7077 --webui-port 8080`

## Worker

`SPARK_LOCAL_IP=127.0.0.1 /usr/lib/jvm/java-8-openjdk/jre/bin/java -cp /home/florian/GIT/sgx-spark/conf/:/home/florian/GIT/sgx-spark/assembly/target/scala-2.11/jars/\* -Xmx1g org.apache.spark.deploy.worker.Worker --webui-port 8081 spark://localhost:7077`

## Enclave side

`/usr/lib/jvm/java-8-openjdk/jre/bin/java -cp /home/florian/GIT/sgx-spark/conf/:/home/florian/GIT/sgx-spark/assembly/target/scala-2.11/jars/\*:examples/target/scala-2.11/jars/spark-examples_2.11-2.3.0-SNAPSHOT.jar org.apache.spark.sgx.SgxMain`

## Spark Job

`SPARK_LOCAL_IP=127.0.0.1 ./bin/spark-submit --class org.apache.spark.examples.JavaWordCount --master spark://localhost:7077 --deploy-mode cluster --executor-memory 1g --name wordcount --conf "spark.app.id=wordcount" examples/target/scala-2.11/jars/spark-examples_2.11-2.3.0-SNAPSHOT.jar file:///home/florian/GIT/sgx-spark/README.md.split 2`
