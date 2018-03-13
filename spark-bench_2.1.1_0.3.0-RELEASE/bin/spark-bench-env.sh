#!/bin/bash

# ############################################################### #
# PLEASE SET THE FOLLOWING VARIABLES TO REFLECT YOUR ENVIRONMENT  #
# ############################################################### #

# set this to the directory where Spark is installed in your environment, for example: /opt/spark-spark-2.1.0-bin-hadoop2.6
export SPARK_HOME=/home/awilliams/sgx-spark

# set this to the master for your environment, such as local[2], yarn, 10.29.0.3, etc.
export SPARK_MASTER_HOST=spark://maruVM:7077

# Further env variables for using spark sgx (with or without sgx mode enabled)

export SGX_USE_SHMEM=true
export SGXLKL_SHMEM_FILE=sgx-lkl-shmem-driver
export SGXLKL_SHMEM_SIZE=10240000
export SGX_ENABLED=true
export PREFETCH=8
export CONNECTIONS=1
export SPARK_LOCAL_IP=127.0.0.1
