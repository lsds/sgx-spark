all:
	gcc -I. -fpic -shared -o libringbuff.so *c -lrt

install: all
	mkdir -p ../lib
	mv libringbuff.so ../lib/

clean:
	rm -rf *.so
