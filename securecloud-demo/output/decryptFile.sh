#!/bin/bash

SPARK_DIR=/home/florian/sgxspark-demo/sgx-spark

filenames=$(ls -1 *)

for file in $filenames; do

	if [[ "${file: -4}" != ".enc" ]]; then
		continue;
	fi

	echo -n "Decrypting file $file ..."
	IS_ENCLAVE=true java -cp "${SPARK_DIR}/assembly/target/scala-2.11/jars/*:${SPARK_DIR}/lkl/scala-library.jar:${SPARK_DIR}/crypttool/target/scala-2.11/jars/spark-crypttool_2.11-2.3.1-SNAPSHOT.jar" org.apache.spark.sgx.Crypttool dec "$file" > "${file::-4}" 2> /dev/null
	echo " done: ${file::-4}"
done

echo ""
echo "Press any key to exit."
read p


