#!/bin/bash

SPARK_MASTER=spark-master
SPARK_WORKERS="spark-slave1 spark-slave2"
USE_SGX_LKL=0

if [ $# -ne 1 ]; then
   echo "Usage: ./$(basename $0) <mode: 0 or 1>"
   exit 1
fi

MODE=$1


# kill old instance
echo "Cleaning existing Spark deployment..."
for w in $SPARK_WORKERS; do
   ssh $w "cd sgx-spark; ./kill_local_spark.sh"
done
./kill_local_spark.sh

sleep 5s

# start master
echo "Start master node"
./master.sh &

sleep 5s

# start each worker
for w in $SPARK_WORKERS; do
   echo "Starting worker node on $w..."
   ssh -n $w "cd sgx-spark; nohup ./worker.sh" &
   sleep 2s

   if [ $USE_SGX_LKL -eq 0 ]; then
      ssh -n $w "cd sgx-spark; nohup ./worker-enclave-nosgx.sh" &
   else
      ssh -n $w "cd sgx-spark; nohup ./worker-enclave.sh" &
   fi

done

sleep 5s
  
# run the job
./submitutfpr.sh $MODE

