sudo docker network rm sgxsparknet; \
sudo docker volume rm sgxshare-input; \
sudo docker network create sgxsparknet; \
sudo docker volume create sgxshare-input; \
export demo=~

sudo docker stop master;sudo  docker rm master; sudo  docker run \
--user user \
--env-file $demo/sgx-spark/dockerfiles/docker-env \
--net sgxsparknet \
--name master \
-v sgxshare-input:/sgx-spark/data/lactec \
-p 7077:7077 \
-p 8082:8082 \
-p 80:80 \
-p 8081:8081 \
-p 4040:4040 \
-ti sgxspark /sgx-spark/master.sh

sudo docker run \
--user user \
--env-file $demo/sgx-spark/dockerfiles/docker-env \
--net sgxsparknet \
--privileged \
-v $demo/sgx-spark/lkl:/spark-image:ro \
-v sgxshare-input:/sgx-spark/data/lactec \
-ti sgxspark /sgx-spark/worker-and-enclave.sh

sudo docker run \
--user user \
--env-file $demo/sgx-spark/dockerfiles/docker-env \
--net sgxsparknet \
--privileged \
-p 18080:18080 \
-v $demo/sgx-spark/lkl:/spark-image:ro \
-v sgxshare-input:/sgx-spark/data/lactec \
-e SPARK_JOB_CLASS=org.apache.spark.examples.lactec.FaultDetection \
-e SPARK_JOB_NAME=lactec.FaultDetection \
-e SPARK_JOB_ARG0=/sgx-spark/data/lactec/Customer.csv.enc \
-e SPARK_JOB_ARG1=/sgx-spark/data/lactec/DSM.csv.enc \
-e SPARK_JOB_ARG2=/sgx-spark/data/lactec/Faults.csv.enc \
-e SPARK_JOB_ARG3=2016-01-01 \
-e SPARK_JOB_ARG4=2016-12-31 \
-e SPARK_JOB_ARG5=/sgx-spark/data/lactec/results.csv.enc \
-ti sgxspark /sgx-spark/driver-and-enclave.sh

docker exec -ti sgxspark-docker-master  tail -n 1 /etc/hosts | sudo tee -a /etc/hosts

# Resolve DNS names
for cont in $(docker ps --filter "network=sgxsparknet" --filter "status=running" -q ); do docker exec -t $cont tail -n 1 /etc/hosts | sudo tee -a /etc/hosts; done

# Stop containers and remove
for cont in $(docker ps --filter "network=sgxsparknet" --filter "status=running" -q ); do docker stop $cont; docker rm $cont; done





