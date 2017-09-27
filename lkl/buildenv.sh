#!/bin/sh

set -ex

PATH=/usr/sbin:/sbin:/usr/bin:/bin
jhome=/opt/j2re-image

cd /home
echo "http://dl-cdn.alpinelinux.org/alpine/v3.6/community" >> /etc/apk/repositories
apk update
apk add iputils iproute2 unzip libstdc++ gcc musl-dev

cd /spark/lib
#gcc -I. -c -fPIC ring_buff.c  -o ring_buff.o
#gcc ring_buff.o -shared -o libring_buff.s
gcc -I. -shared -fpic -o libringbuff.so *.c


# unzip ${jhome}/lib/rt.jar -d /tmp/exploded-rt-jar
# rm ${jhome}/lib/rt.jar
# mv /tmp/exploded-rt-jar ${jhome}/lib/rt.jar
# 
# ## Trying to unzip jars to class files -- remove all of the below when jar loading works
# # Unzip Spark Core -- because we don't yet support jars :(
# mkdir -p /spark/assembly/target/scala-2.11/jars/spark-core
# unzip /spark/assembly/target/scala-2.11/jars/spark-core_2.11-2.3.0-SNAPSHOT.jar -d /spark/assembly/target/scala-2.11/jars/spark-core
# 
# # Unzip Hadoop Common -- because we don't yet support jars :(
# mkdir -p /spark/assembly/target/scala-2.11/jars/hadoop-common
# unzip /spark/assembly/target/scala-2.11/jars/hadoop-common-2.6.5.jar -d /spark/assembly/target/scala-2.11/jars/hadoop-common
# 
# # Move all classes to the jar directory
# find /spark/assembly/target/scala-2.11/jars "*.class" -type f -exec cp {} /spark/assembly/target/scala-2.11/jars \; | true

