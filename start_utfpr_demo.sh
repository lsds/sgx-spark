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
for w in $SPARK_MASTER_HOST $SPARK_WORKERS_HOST; do
   ssh $w "cd sgx-spark; ./kill_local_spark.sh"
done
./kill_local_spark.sh

# start master
echo "Start master node"
./master.sh &

# re-create KVS
# TODO put these lines in the submit script
if [ $MODE -eq 0 ]; then
   curl -k  -d 'name=sparkdemo&storage_policy_name=sparkdemo_sp' -H "Authorization: ApiKey 0131Byd7N220T32qp088kIT53ryT113i0123456789012345" -H "Content-Type: application/x-www-form-urlencoded" -X POST https://kvs.lsd.ufcg.edu.br:7778/datastores/
   curl -k -H "Authorization: ApiKey 0131Byd7N220T32qp088kIT53ryT113i0123456789012345" -X "DELETE" https://kvs.lsd.ufcg.edu.br:7778/datastores/sparkdemo/
fi

# start each worker
for w in $SPARK_WORKERS_HOST; do
   echo "Starting worker node on $w..."
   ssh -n $w "cd sgx-spark; nohup ./worker.sh" &
   sleep 2s

   if [ $USE_SGX_LKL -eq 0 ]; then
      ssh -n $w "cd sgx-spark; nohup ./worker-enclave-nosgx.sh" &
   else
      ssh -n $w "cd sgx-spark; nohup ./worker-enclave.sh" &
   fi

done
  
# run the job
./submitutfpr.sh $MODE

