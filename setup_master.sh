SPARK_LOCAL_IP=127.0.0.1 java -cp conf/:assembly/target/scala-2.11/jars/\*:$(pwd)/lib/* -Xmx1g org.apache.spark.deploy.master.Master --host maruVM --port 7077 --webui-port 8080
