#!/bin/sh

set -ex

PATH=/usr/sbin:/sbin:/usr/bin:/bin

cat /etc/apk/repositories

echo "http://dl-cdn.alpinelinux.org/alpine/v3.7/community" >> /etc/apk/repositories

apk update
apk add iputils iproute2 unzip libstdc++ gcc musl-dev

cd /spark/lib
gcc -I. -shared -fpic -o libringbuff.so *.c

ln -s /opt/j2re-image/lib/amd64/server/libjvm.so /usr/local/lib
