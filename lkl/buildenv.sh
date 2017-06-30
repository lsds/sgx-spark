#!/bin/sh

set -ex

PATH=/usr/sbin:/sbin:/usr/bin:/bin
jhome=/opt/j2re-image

cd /home
echo "http://dl-cdn.alpinelinux.org/alpine/v3.6/community" >> /etc/apk/repositories
apk update
apk add iputils iproute2 unzip libstdc++

unzip ${jhome}/lib/rt.jar -d /tmp/exploded-rt-jar
rm ${jhome}/lib/rt.jar
mv /tmp/exploded-rt-jar ${jhome}/lib/rt.jar
